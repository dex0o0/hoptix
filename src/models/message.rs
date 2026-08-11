use serde::{Deserialize, Serialize};

use crate::service::download::DownloadStatus;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Command {
    Start { url: String, output: Option<String> },
    Status,
    Pause,
    Resume,
    Stop { id: u64 },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Response {
    Started {
        id: u64,
    },
    Progress {
        total: u64,
        downloaded: u64,
        speed: f64,
    },
    Finished,
    Error(String),
    Cancelled {
        id: u64,
    },
    Status {
        downloads: Vec<DownloadStatus>,
    },
}
