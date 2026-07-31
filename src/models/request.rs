use crate::error::{AppError, Result};
use reqwest::{self, Client};

pub async fn check_server_support(url: &str) -> Result<u64> {
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
