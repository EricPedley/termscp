use std::path::{Path, PathBuf};
use std::thread::{self, JoinHandle};

use remotefs::fs::{File, Metadata, RemoteFs};
use tempfile::NamedTempFile;

use super::{FileTransferActivity, LogLevel};
use crate::filetransfer::{
    FileTransferParams, HostBridgeBuilder, HostBridgeParams, RemoteFsBuilder,
};
use crate::host::HostBridge;

pub(super) struct DownloadJob {
    label: String,
    handle: JoinHandle<DownloadJobResult>,
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
        let worker_label = label.clone();
        self.log(LogLevel::Info, format!("Started download in background: {label}"));
        self.download_jobs.push(DownloadJob {
            label,
            handle: thread::spawn(move || DownloadJobResult {
                label: worker_label,
                result: download_entry_job(host_bridge_params, remote_params, entry, host_bridge_path, dst_name),
            }),
        });
    }

    pub(super) fn poll_download_jobs(&mut self) {
        let mut i = 0;
        let mut should_reload = false;
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
        }
    }
}

fn download_entry_job(
    host_bridge_params: HostBridgeParams,
    remote_params: FileTransferParams,
    entry: File,
    host_bridge_path: PathBuf,
    dst_name: Option<String>,
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
            )?;
        }
        return Ok(());
    }

    let mut host_bridge_file_path = PathBuf::from(host_bridge_path);
    host_bridge_file_path.push(dst_name.unwrap_or_else(|| entry.name()));
    download_file(client, host_bridge, host_bridge_file_path.as_path(), entry)
}

fn download_file(
    client: &mut dyn RemoteFs,
    host_bridge: &mut dyn HostBridge,
    host_bridge_path: &Path,
    remote: &File,
) -> Result<(), String> {
    let temp = NamedTempFile::new().map_err(|e| e.to_string())?;
    let temp_path = temp.path().to_path_buf();

    match client.open(remote.path()) {
        Ok(mut reader) => {
            let mut writer = std::fs::File::create(temp_path.as_path()).map_err(|e| e.to_string())?;
            std::io::copy(&mut reader, &mut writer).map_err(|e| e.to_string())?;
            client.on_read(reader).map_err(|e| e.to_string())?;
        }
        Err(err) if err.kind == remotefs::RemoteErrorType::UnsupportedFeature => {
            let writer = std::fs::File::create(temp_path.as_path()).map_err(|e| e.to_string())?;
            client
                .open_file(remote.path(), Box::new(writer))
                .map_err(|e| e.to_string())?;
        }
        Err(err) => return Err(err.to_string()),
    }

    let mut temp_reader = std::fs::File::open(temp_path.as_path()).map_err(|e| e.to_string())?;
    let mut writer = host_bridge
        .create_file(host_bridge_path, &remote.metadata)
        .map_err(|e| e.to_string())?;
    std::io::copy(&mut temp_reader, &mut writer).map_err(|e| e.to_string())?;
    host_bridge.finalize_write(writer).map_err(|e| e.to_string())?;
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
