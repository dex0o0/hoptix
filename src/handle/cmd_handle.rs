use crate::{
    config::AppConf,
    error::{AppError, Result},
    models::{
        file_prop::prepare_file,
        message::{Command, Response},
        request::inspect_url,
    },
    routes::netcard,
    service::download::DownloadStatus,
    AppState, SockConnect,
};
use std::{
    path::PathBuf,
    sync::{atomic::Ordering, Arc},
};
use tokio::{net::UnixStream, sync::Mutex};

pub struct CommandHandle {
    pub state: Arc<Mutex<AppState>>,
    pub config: Arc<AppConf>,
    pub stream: Arc<Mutex<UnixStream>>,
}

impl CommandHandle {
    pub async fn handle(&self, command: Command) -> Result<()> {
        match command {
            Command::Start { url, output } => {
                let id = self.start(url, output).await?;
                let mut stream = self.stream.lock().await;

                SockConnect::send_response(&mut stream, &Response::Started { id })
                    .await
                    .map_err(|e| AppError::IpcError(e.to_string()))?;
            }
            Command::Status => {
                let status = self.status().await;

                let mut stream = self.stream.lock().await;
                SockConnect::send_response(&mut stream, &Response::Status { downloads: status })
                    .await
                    .map_err(|e| AppError::IpcError(e.to_string()))?;
            }
            Command::Pause => {
                todo!()
            }
            Command::Resume => {
                todo!()
            }
            Command::Stop { id } => {
                let state = self.state.lock().await;
                state.manager.cancel(id)?;
                let mut stream = self.stream.lock().await;
                SockConnect::send_response(&mut stream, &Response::Cancelled { id })
                    .await
                    .map_err(|e| AppError::IpcError(e.to_string()))?;
            }
        }
        Ok(())
    }
    async fn start(&self, url: String, output: Option<String>) -> Result<u64> {
        let file_info = inspect_url(&url).await?;
        let output = match output {
            Some(output) => PathBuf::from(output),
            None => {
                let filename = file_info.filename.unwrap_or_else(|| "download".to_string());
                self.config.download_dir.join(filename)
            }
        };

        let output = prepare_file(&output, file_info.size).await?;

        let interface = netcard::get_working_interface(&url).await?;

        let mut state = self.state.lock().await;
        let id = state
            .manager
            .start_download(
                file_info.url,
                output,
                file_info.size,
                file_info.support_range,
                interface,
                self.config.clone(),
            )
            .await?;

        Ok(id)
    }
    async fn status(&self) -> Vec<DownloadStatus> {
        let mut state = self.state.lock().await;
        state.manager.cleanup();
        let mut result = Vec::new();
        for session in state.manager.get_all() {
            let downloaded = session.downloaded.load(Ordering::SeqCst);
            let state_val = *session.state.lock().await;

            result.push(DownloadStatus {
                id: session.id,
                total: session.total,
                downloaded,
                progress: session.progress(),
                elapsed: session.elapsed(),
                state: state_val,
            });
        }
        result
    }
}
