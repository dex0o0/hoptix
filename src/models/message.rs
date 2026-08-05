use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Command {
    Start { url: String, output: Option<String> },
    Status,
    Pause,
    Resume,
    Stop,
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
}
