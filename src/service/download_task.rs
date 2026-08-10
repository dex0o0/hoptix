use crate::{
    config::AppConf,
    error::Result,
    event::event::DownloadEvent,
    routes::netcard::NetworkInterface,
    service::download::{download_file_parallel, download_single},
};
use std::{
    path::PathBuf,
    sync::{Arc, atomic::AtomicU64},
};
use tokio::{sync::mpsc, task::JoinHandle};

pub struct DownloadTask {
    pub id: u64,
    pub url: String,
    pub output: PathBuf,
    pub size: u64,
    pub downloaded: Arc<AtomicU64>,
    pub support_range: bool,
    pub interface: Vec<NetworkInterface>,
    pub progress_tx: mpsc::Sender<DownloadEvent>,
    pub config: Arc<AppConf>,
}

impl DownloadTask {
    pub fn new(
        id: u64,
        url: String,
        output: PathBuf,
        size: u64,
        support_range: bool,
        downloaded: Arc<AtomicU64>,
        interface: Vec<NetworkInterface>,
        progress_tx: mpsc::Sender<DownloadEvent>,
        config: Arc<AppConf>,
    ) -> Self {
        Self {
            id,
            url,
            output,
            size,
            support_range,
            interface,
            downloaded,
            progress_tx,
            config,
        }
    }
    pub fn start(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let id = self.id;

            match self.run().await {
                Ok(_) => {
                    let _ = self.progress_tx.send(DownloadEvent::Finished { id }).await;
                }
                Err(e) => {
                    let _ = self
                        .progress_tx
                        .send(DownloadEvent::Error {
                            id,
                            error: e.to_string(),
                        })
                        .await;
                }
            }
        })
    }
    async fn run(&self) -> Result<()> {
        if !self.support_range {
            println!("DownloadTask: using single connection");
            download_single(
                self.id,
                &self.url,
                &self.output,
                self.downloaded.clone(),
                self.progress_tx.clone(),
                self.size,
            )
            .await?;
            return Ok(());
        }
        println!("DownloadTask: using parallel download");

        download_file_parallel(
            self.id,
            &self.url,
            self.size,
            self.interface.clone(),
            &self.output,
            self.progress_tx.clone(),
            self.downloaded.clone(),
            self.config.chunk,
            self.config.thread,
        )
        .await?;

        Ok(())
    }
}
