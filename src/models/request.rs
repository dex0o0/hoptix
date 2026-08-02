use std::net::IpAddr;

use crate::error::{AppError, Result};
use reqwest::{self, Client};

#[derive(Debug, Clone)]
pub struct Chunck {
    pub id: usize,
    pub start: u64,
    pub end: u64,
    pub downloaded_bytes: u64,
    pub assigned_ip: Option<IpAddr>,
}

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub url: String,
    pub size: u64,
    pub support_range: bool,
}

pub async fn check_range_support(url: &str) -> Result<u64> {
    let client = Client::new();
    let response = client.head(url).send().await?;

    let accept_range = response
        .headers()
        .get("accept-ranges")
        .and_then(|v| v.to_str().ok());

    if accept_range != Some("bytes") {
        return Err(AppError::RangeNotSupported);
    }

    let content_length = response
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);

    Ok(content_length)
}
