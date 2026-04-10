use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::{self, JoinHandle};

use remotefs::fs::{File, Metadata, RemoteFs};
use tempfile::NamedTempFile;

use super::{FileTransferActivity, LogLevel};
use crate::filetransfer::{
    FileTransferParams, HostBridgeBuilder, HostBridgeParams, RemoteFsBuilder,
};
use crate::host::HostBridge;
use crate::utils::fmt::fmt_path_elide_ex;

pub(super) struct DownloadJob {
    pub(super) label: String,
    pub(super) progress: Arc<DownloadProgress>,
    handle: JoinHandle<DownloadJobResult>,
}

pub(super) struct DownloadProgress {
    pub(super) display_name: String,
    total_bytes: u64,
    written_bytes: AtomicU64,
}

impl DownloadProgress {
    fn new(display_name: String, total_bytes: u64) -> Self {
        Self {
            display_name,
            total_bytes,
            written_bytes: AtomicU64::new(0),
        }
    }

    fn add_progress(&self, delta: u64) {
        self.written_bytes.fetch_add(delta, Ordering::Relaxed);
    }

    pub(super) fn render_line(&self, width: usize) -> String {
        let progress = self.written_bytes.load(Ordering::Relaxed);
        let percent = if self.total_bytes == 0 {
            0
        } else {
            ((progress.saturating_mul(100)) / self.total_bytes).min(100)
        };
        let suffix = format!(" {:>3}%", percent);
        let name = fmt_path_elide_ex(Path::new(&self.display_name), width, suffix.len() + 3);
        format!("{}{}", name, suffix)
    }
}

struct DownloadJobResult {
    label: String,
    result: Result<(), String>,
}

impl FileTransferActivity {
    pub(super) fn spawn_download_job(
        &mut self,
        entry: File,
        host_bridge_path: PathBuf,
        dst_name: Option<String>,
    ) {
        let host_bridge_params = self.context().host_bridge_params().unwrap().clone();
        let remote_params = self.context().remote_params().unwrap().clone();
        let label = match &dst_name {
            Some(name) => format!("{} -> {}", entry.path().display(), name),
            None => entry.path().display().to_string(),
        };
        let display_name = dst_name.clone().unwrap_or_else(|| entry.name());
        let total_bytes = if entry.is_dir() {
            self.get_total_transfer_size_remote(&entry) as u64
        } else {
            entry.metadata().size
        };
        let progress = Arc::new(DownloadProgress::new(display_name, total_bytes));
        let worker_label = label.clone();
        let worker_progress = Arc::clone(&progress);
        self.log(LogLevel::Info, format!("Started download in background: {label}"));
        self.download_jobs.push(DownloadJob {
            label,
            progress,
            handle: thread::spawn(move || DownloadJobResult {
                label: worker_label,
                result: download_entry_job(
                    host_bridge_params,
                    remote_params,
                    entry,
                    host_bridge_path,
                    dst_name,
                    worker_progress,
                ),
            }),
        });
    }

    pub(super) fn poll_download_jobs(&mut self) {
        let mut i = 0;
        let mut should_reload = false;
        let has_active_jobs = !self.download_jobs.is_empty();
        while i < self.download_jobs.len() {
            if !self.download_jobs[i].handle.is_finished() {
                i += 1;
                continue;
            }

            let job = self.download_jobs.swap_remove(i);
            match job.handle.join() {
                Ok(DownloadJobResult { label, result: Ok(()) }) => {
                    self.log(LogLevel::Info, format!("Download completed: {label}"));
                    should_reload = true;
                }
                Ok(DownloadJobResult { label, result: Err(err) }) => {
                    self.log_and_alert(LogLevel::Error, format!("Download failed for {label}: {err}"));
                    should_reload = true;
                }
                Err(_) => {
                    self.log_and_alert(
                        LogLevel::Error,
                        format!("Download worker panicked: {}", job.label),
                    );
                    should_reload = true;
                }
            }
        }

        if should_reload {
            self.reload_host_bridge_dir();
            self.reload_host_bridge_filelist();
            self.redraw = true;
        } else if has_active_jobs {
            self.reload_host_bridge_filelist();
            self.redraw = true;
        }
    }
}

fn download_entry_job(
    host_bridge_params: HostBridgeParams,
    remote_params: FileTransferParams,
    entry: File,
    host_bridge_path: PathBuf,
    dst_name: Option<String>,
    progress: Arc<DownloadProgress>,
) -> Result<(), String> {
    let config_client = FileTransferActivity::init_config_client();
    let mut host_bridge = HostBridgeBuilder::build(host_bridge_params, &config_client)?;
    let mut client = RemoteFsBuilder::build(remote_params.protocol, remote_params.params, &config_client)?;

    if !host_bridge.is_connected() {
        host_bridge.connect().map_err(|e| e.to_string())?;
    }
    if !client.is_connected() {
        client.connect().map_err(|e| e.to_string())?;
    }

    let result = download_entry(
        client.as_mut(),
        host_bridge.as_mut(),
        &entry,
        host_bridge_path.as_path(),
        dst_name,
        progress,
    );

    let _ = client.disconnect();
    let _ = host_bridge.disconnect();

    result
}

fn download_entry(
    client: &mut dyn RemoteFs,
    host_bridge: &mut dyn HostBridge,
    entry: &File,
    host_bridge_path: &Path,
    dst_name: Option<String>,
    progress: Arc<DownloadProgress>,
) -> Result<(), String> {
    if entry.is_dir() {
        let mut host_bridge_dir_path = PathBuf::from(host_bridge_path);
        match dst_name {
            Some(name) => host_bridge_dir_path.push(name),
            None => host_bridge_dir_path.push(entry.name()),
        }
        host_bridge
            .mkdir_ex(host_bridge_dir_path.as_path(), true)
            .map_err(|e| e.to_string())?;
        let _ = host_bridge.setstat(host_bridge_dir_path.as_path(), entry.metadata());
        for child in client.list_dir(entry.path()).map_err(|e| e.to_string())? {
            download_entry(
                client,
                host_bridge,
                &child,
                host_bridge_dir_path.as_path(),
                None,
                Arc::clone(&progress),
            )?;
        }
        return Ok(());
    }

    let mut host_bridge_file_path = PathBuf::from(host_bridge_path);
    host_bridge_file_path.push(dst_name.unwrap_or_else(|| entry.name()));
    download_file(
        client,
        host_bridge,
        host_bridge_file_path.as_path(),
        entry,
        progress,
    )
}

fn download_file(
    client: &mut dyn RemoteFs,
    host_bridge: &mut dyn HostBridge,
    host_bridge_path: &Path,
    remote: &File,
    progress: Arc<DownloadProgress>,
) -> Result<(), String> {
    match client.open(remote.path()) {
        Ok(mut reader) => {
            let writer = host_bridge
                .create_file(host_bridge_path, &remote.metadata)
                .map_err(|e| e.to_string())?;
            let mut writer = CountingWriter::new(writer, Arc::clone(&progress));
            copy_stream(&mut reader, &mut writer)?;
            client.on_read(reader).map_err(|e| e.to_string())?;
            host_bridge
                .finalize_write(writer.into_inner())
                .map_err(|e| e.to_string())?;
        }
        Err(err) if err.kind == remotefs::RemoteErrorType::UnsupportedFeature => {
            let tmp = NamedTempFile::new().map_err(|e| e.to_string())?;
            let temp_path = tmp.path().to_path_buf();
            let temp_file = std::fs::File::create(temp_path.as_path()).map_err(|e| e.to_string())?;
            let writer = CountingWriter::new(temp_file, Arc::clone(&progress));
            client
                .open_file(remote.path(), Box::new(writer))
                .map_err(|e| e.to_string())?;
            let mut temp_reader =
                std::fs::File::open(temp_path.as_path()).map_err(|e| e.to_string())?;
            let mut writer = host_bridge
                .create_file(host_bridge_path, &remote.metadata)
                .map_err(|e| e.to_string())?;
            copy_stream(&mut temp_reader, &mut writer)?;
            host_bridge
                .finalize_write(writer)
                .map_err(|e| e.to_string())?;
        }
        Err(err) => return Err(err.to_string()),
    }

    apply_host_bridge_stat(host_bridge, host_bridge_path, remote.metadata())?;

    Ok(())
}

fn apply_host_bridge_stat(
    host_bridge: &mut dyn HostBridge,
    path: &Path,
    metadata: &Metadata,
) -> Result<(), String> {
    host_bridge.setstat(path, metadata).map_err(|e| e.to_string())
}

fn copy_stream(
    reader: &mut dyn std::io::Read,
    writer: &mut dyn std::io::Write,
) -> Result<(), String> {
    let mut buf = [0u8; 65535];
    loop {
        let bytes_read = reader.read(&mut buf).map_err(|e| e.to_string())?;
        if bytes_read == 0 {
            return Ok(());
        }
        writer
            .write_all(&buf[..bytes_read])
            .map_err(|e| e.to_string())?;
    }
}

struct CountingWriter<W> {
    inner: W,
    progress: Arc<DownloadProgress>,
}

impl<W> CountingWriter<W> {
    fn new(inner: W, progress: Arc<DownloadProgress>) -> Self {
        Self { inner, progress }
    }

    fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: std::io::Write> std::io::Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.progress.add_progress(written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.inner.write_all(buf)?;
        self.progress.add_progress(buf.len() as u64);
        Ok(())
    }
}
