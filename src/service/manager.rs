use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::mpsc::{self, Sender},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    config::AppConf,
    error::{AppError, Result},
    event::event::DownloadEvent,
    routes::netcard::NetworkInterface,
    service::download_task::DownloadTask,
};

pub struct DownloadSession {
    pub id: u64,
    pub total: u64,
    pub downloaded: Arc<AtomicU64>,
    pub start_time: Instant,
    pub handle: Option<JoinHandle<()>>,
    pub cancel_token: CancellationToken,
}

pub struct DownloadManager {
    pub downloads: HashMap<u64, DownloadSession>,
    pub event_sender: mpsc::Sender<DownloadEvent>,
    pub next_id: u64,
}

impl DownloadManager {
    pub fn new(event_sender: mpsc::Sender<DownloadEvent>) -> Self {
        Self {
            downloads: HashMap::new(),
            event_sender,
            next_id: 0,
        }
    }
    pub async fn start_download(
        &mut self,
        url: String,
        output: PathBuf,
        size: u64,
        support_range: bool,
        interface: Vec<NetworkInterface>,
        // event_tx: Sender<DownloadEvent>,
        config: Arc<AppConf>,
    ) -> Result<u64> {
        let id = self.next_id();
        let downloaded = Arc::new(AtomicU64::new(0));
        let cancel_token = CancellationToken::new();

        let mut session = DownloadSession::new(id, size, downloaded.clone(), cancel_token.clone());

        let task = DownloadTask::new(
            id,
            url,
            output,
            size,
            support_range,
            downloaded,
            interface,
            self.event_sender.clone(),
            config,
            cancel_token,
        );

        let handle = task.start();
        session.handle = Some(handle);

        self.add(session);
        self.emit(DownloadEvent::Started { id }).await?;
        Ok(id)
    }

    pub fn add(&mut self, session: DownloadSession) {
        self.downloads.insert(session.id, session);
    }

    pub fn get(&self, id: &u64) -> Option<&DownloadSession> {
        self.downloads.get(id)
    }

    pub fn next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    async fn emit(&self, event: DownloadEvent) -> Result<()> {
        self.event_sender
            .send(event)
            .await
            .map_err(|_| AppError::ChannelClose)?;

        Ok(())
    }

    pub fn statuses(&self) -> Vec<&DownloadSession> {
        self.downloads.values().collect()
    }

    pub fn get_all(&self) -> impl Iterator<Item = &DownloadSession> {
        self.downloads.values()
    }

    pub fn cancel(&self, id: u64) -> Result<()> {
        let session = self
            .downloads
            .get(&id)
            .ok_or_else(|| AppError::IpcError(format!("Download {} not found", id)))?;

        session.cancel_token.cancel();

        Ok(())
    }
    pub fn cleanup(&mut self) {
        self.downloads.retain(|_, session| match &session.handle {
            Some(handle) => !handle.is_finished(),
            None => false,
        });
    }
}

impl DownloadSession {
    pub fn new(
        id: u64,
        total: u64,
        downloaded: Arc<AtomicU64>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            id,
            total,
            downloaded,
            start_time: Instant::now(),
            handle: None,
            cancel_token,
        }
    }

    pub fn progress(&self) -> f64 {
        let downloaded = self.downloaded.load(Ordering::SeqCst);

        if self.total == 0 {
            return 0.0;
        }
        downloaded as f64 / self.total as f64 * 100.0
    }

    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }
}
