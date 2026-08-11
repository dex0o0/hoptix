#[derive(Debug)]
pub enum DownloadEvent {
    Started {
        id: u64,
    },
    Progress {
        id: u64,
        downloaded: u64,
        total: u64,
    },
    Finished {
        id: u64,
    },
    Error {
        id: u64,
        error: String,
    },
    Cancelled {
        id: u64,
    },
}
