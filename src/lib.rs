use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt},
    net::UnixStream,
};

use crate::models::message::{Command, Response};

pub mod config;
pub mod error;
pub mod models;
pub mod routes;
pub mod service;

pub struct SockConnect;

impl SockConnect {
    pub async fn send_command(
        stream: &mut UnixStream,
        cmd: &Command,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let json = serde_json::to_string(cmd)?;
        stream.write_all(json.as_bytes()).await?;
        stream.write_all(b"\n").await?;
        let _ = stream.flush().await;

        Ok(())
    }

    pub async fn read_command(
        stream: &mut UnixStream,
    ) -> Result<Command, Box<dyn std::error::Error>> {
        let mut reader = tokio::io::BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        let cmd: Command = serde_json::from_str(&line)?;
        Ok(cmd)
    }

    pub async fn read_response(
        stream: &mut UnixStream,
    ) -> Result<Response, Box<dyn std::error::Error>> {
        let mut reader = tokio::io::BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        let response: Response = serde_json::from_str(&line)?;
        Ok(response)
    }

    pub async fn send_response(
        stream: &mut UnixStream,
        resp: &Response,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let json = serde_json::to_string(resp)?;
        stream.write_all(json.as_bytes()).await?;
        stream.write_all(b"\n").await?;
        stream.flush().await?;
        Ok(())
    }
}
