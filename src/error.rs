use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Error File System / Inpute-Output: {0}")]
    Io(#[from] io::Error),

    #[error("Network Error With HTTP: {0}")]
    Network(#[from] reqwest::Error),

    #[error("Server Not Supported From Range Bytes")]
    RangeNotSupported,

    #[error("Unspecified file size or is zero")]
    InvalidFileSize,

    #[error("Network Card Target Not Found ({0})")]
    NetcardNotFound(String),

    #[error("Download Cache Invalid")]
    InvalidCache,

    #[error("Error Connected To Socket:{0}")]
    IpcError(String),
}

pub type Result<T> = std::result::Result<T, AppError>;
