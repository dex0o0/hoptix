use crate::error::{AppError, Result};
use reqwest::{self, Client};

pub async fn check_server_support(url: &str) -> Result<bool> {}
