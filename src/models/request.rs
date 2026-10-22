use crate::error::{AppError, Result};
use reqwest::{self, Client};
use std::net::IpAddr;
use url::Url;

#[derive(Debug, Clone)]
pub struct Chunk {
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
    pub filename: Option<String>,
}

pub fn is_localhost(url: &str) -> bool {
    if let Ok(parsed) = Url::parse(url)
        && let Some(host) = parsed.host_str()
    {
        return host == "localhost" || host == "127.0.0.1";
    }
    false
}

pub fn filename_from_server(value: &str) -> Option<String> {
    for part in value.split(';') {
        let part = part.trim();

        if let Some(filename) = part.strip_prefix("filename=") {
            return Some(filename.trim_matches('"').to_string());
        }
    }
    None
}

pub async fn inspect_url(url: &str) -> Result<FileInfo> {
    let client = Client::new();
    let response = client.head(url).send().await?;

    let support_range = response
        .headers()
        .get("accept-ranges")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("bytes"))
        .unwrap_or(false);

    let size = response
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);

    if size == 0 {
        return Err(AppError::InvalidFileSize);
    }

    let filename = response
        .headers()
        .get("content-disposition")
        .and_then(|v| v.to_str().ok())
        .and_then(filename_from_server);

    Ok(FileInfo {
        url: url.to_string(),
        size,
        support_range,
        filename,
    })
}

#[allow(dead_code)]
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
