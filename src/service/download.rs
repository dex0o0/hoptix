use crate::error::{AppError, Result};
use crate::event::event::DownloadEvent;
use crate::models::request::{check_range_support, Chunk, FileInfo};
use crate::routes::netcard::NetworkInterface;
use futures::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{collections::VecDeque, net::IpAddr, path::Path, sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio::{
    fs::OpenOptions,
    io::{AsyncSeekExt, AsyncWriteExt, SeekFrom},
    sync::Mutex,
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadStatus {
    pub id: u64,
    pub total: u64,
    pub downloaded: u64,
    pub progress: f64,
    pub elapsed: Duration,
}

pub async fn inspect_url(url: &str) -> Result<FileInfo> {
    let client = Client::new();
    let response = client.head(url).send().await?;

    let support_range = check_range_support(url).await.is_ok();

    let size = response
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    if size == 0 {
        return Err(AppError::InvalidFileSize);
    }
    Ok(FileInfo {
        url: url.to_string(),
        size,
        support_range,
    })
}

pub fn create_chunk(file_size: u64, num_chunk: usize) -> Vec<Chunk> {
    const MIN_CHUNK_SIZE: u64 = 2 * 1024 * 1024;

    let optimal = (file_size / MIN_CHUNK_SIZE) as usize;
    let num_chunk = optimal.clamp(1, num_chunk);

    let mut chunks = Vec::new();

    if num_chunk == 0 || file_size == 0 {
        return chunks;
    }
    let num_chunk_u64 = num_chunk as u64;
    let base_chunk_size = file_size / num_chunk_u64;

    for i in 0..num_chunk {
        let start = (i as u64) * base_chunk_size;

        let end = if i == num_chunk - 1 {
            file_size - 1
        } else {
            ((i as u64) + 1) * base_chunk_size - 1
        };

        chunks.push(Chunk {
            id: i,
            start,
            end,
            downloaded_bytes: 0,
            assigned_ip: None,
        });
    }
    chunks
}

pub async fn download_chunk(
    url: &str,
    chunk: Chunk,
    bound_ip: IpAddr,
    file_path: &Path,
) -> Result<()> {
    let client = Client::builder()
        .local_address(bound_ip)
        .connect_timeout(Duration::from_secs(5))
        .read_timeout(Duration::from_secs(15))
        .pool_max_idle_per_host(10)
        .build()?;

    let range_header = format!("bytes={}-{}", chunk.start, chunk.end);
    let mut response = client.get(url).header("Range", range_header).send().await?;

    if !response.status().is_success() {
        return Err(AppError::RangeNotSupported);
    }

    let mut file = OpenOptions::new()
        .write(true)
        .open(file_path)
        .await
        .map_err(AppError::Io)?;

    file.seek(SeekFrom::Start(chunk.start))
        .await
        .map_err(AppError::Io)?;

    while let Some(data_chunk) = response.chunk().await? {
        file.write_all(&data_chunk).await.map_err(AppError::Io)?;
    }

    Ok(())
}

pub async fn download_file_parallel(
    id: u64,
    url: &str,
    file_size: u64,
    interface: Vec<NetworkInterface>,
    output_path: &Path,
    progress_tx: mpsc::Sender<DownloadEvent>,
    downloaded: Arc<AtomicU64>,
    num_chunk: usize,
    num_threads: usize,
    cancel_token: CancellationToken,
) -> Result<()> {
    eprintln!("download_file_parallel started");
    let chunks = create_chunk(file_size, num_chunk);
    let chunk_queue = Arc::new(Mutex::new(VecDeque::from(chunks)));

    let total = file_size;
    let progress_tx_clone = progress_tx.clone();
    let downloaded_clone = downloaded.clone();
    let cancel = cancel_token.clone();
    tokio::spawn(async move {
        let mut last_downloaded = 0u64;
        let mut last_time = std::time::Instant::now();
        loop {
            tokio::select! {
                _ = cancel.cancelled()=>{break;}
                _ = tokio::time::sleep(Duration::from_millis(500)) =>{
            let current = downloaded_clone.load(Ordering::SeqCst);
            let now = std::time::Instant::now();
            let elapsed = now.duration_since(last_time).as_secs_f64();
            let speed = if elapsed > 0.0 {
                (current - last_downloaded) as f64 / elapsed
            } else {
                0.0
            };
            let _ = progress_tx_clone
                .send(DownloadEvent::Progress {
                    id,
                    downloaded: current,
                    total,
                })
                .await;
            last_downloaded = current;
            last_time = now;
            if current >= total {
                break;
            }
            }}
        }
    });

    let mut joinset = JoinSet::new();

    let interfaces = if interface.is_empty() {
        vec![NetworkInterface {
            name: "default".to_string(),
        }]
    } else {
        interface
    };

    for iface in interfaces {
        for _thread_id in 0..num_threads {
            let queue = Arc::clone(&chunk_queue);
            let url = url.to_string();
            let path = output_path.to_path_buf();
            let iface = iface.clone();
            let download_clone = downloaded.clone();
            let cancel = cancel_token.clone();
            joinset.spawn(async move {
                // let client = match Client::builder()
                //     .interface(&iface.name)
                //     .connect_timeout(Duration::from_secs(5))
                //     .read_timeout(Duration::from_secs(15))
                //     .pool_max_idle_per_host(10)
                //     .build()
                // {
                //     Ok(c) => c,
                //     Err(e) => {
                //         eprintln!(
                //             "can not create client for [{}] (message::Error::{e})",
                //             iface.name
                //         );
                //         return;
                //     }
                // };

                let builder = Client::builder()
                    .connect_timeout(Duration::from_secs(5))
                    .read_timeout(Duration::from_secs(15))
                    .pool_max_idle_per_host(10);

                let client = match if iface.name == "default" {
                    builder.build()
                } else {
                    builder.interface(&iface.name).build()
                } {
                    Ok(client) => client,
                    Err(e) => {
                        eprintln!("can not create client for [{}]:{}", iface.name, e);
                        return;
                    }
                };

                let mut faile_count = 0;
                const MX_FAILS: u32 = 3;
                loop {
                    if cancel.is_cancelled() {
                        break;
                    }
                    if faile_count >= MX_FAILS {
                        break;
                    }
                    let maybe_chunk = {
                        let mut lock = queue.lock().await;
                        lock.pop_front()
                    };

                    let chunk = match maybe_chunk {
                        Some(c) => c,
                        None => break,
                    };

                    println!(
                        "[{}] downloading {} (bytes={}-{})",
                        iface.name, chunk.id, chunk.start, chunk.end
                    );

                    if let Err(e) = download_chunk_with_client(
                        &client,
                        &url,
                        chunk.clone(),
                        &path,
                        download_clone.clone(),
                        cancel.clone(),
                    )
                    .await
                    {
                        faile_count += 1;
                        eprintln!(
                            "Error on [{}] to download {}: (error message:{:?})",
                            iface.name, chunk.id, e
                        );
                        let mut lock = queue.lock().await;
                        lock.push_back(chunk);
                        drop(lock);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    } else {
                        faile_count = 0;
                        println!("chonck:{} downloading with [{}]", chunk.id, iface.name);
                    }
                    println!();
                }
            });
        }
    }

    while let Some(res) = joinset.join_next().await {
        res.map_err(|e| AppError::IpcError(e.to_string()))?;
    }

    downloaded.store(file_size, Ordering::SeqCst);

    Ok(())
}

pub async fn download_chunk_with_client(
    client: &Client,
    url: &str,
    chunk: Chunk,
    file_path: &Path,
    downloaded: Arc<AtomicU64>,
    cancel_token: CancellationToken,
) -> Result<()> {
    let range_header = format!("bytes={}-{}", chunk.start, chunk.end);
    let response = client.get(url).header("Range", range_header).send().await?;

    if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
        return Err(AppError::RangeNotSupported);
    }

    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(file_path)
        .map_err(AppError::Io)?;

    let mut offset = chunk.start;

    let mut stream = response.bytes_stream();

    loop {
        tokio::select! {
            _ = cancel_token.cancelled()=> {
                return Ok(());
            }
            item = stream.next()=>{
                let item = match item {
                    Some(i)=>i?,
                    None=>break,
                };
                let data = item;

                file.write_at(&data, offset).map_err(AppError::Io)?;

                offset += data.len() as u64;

                downloaded.fetch_add(data.len() as u64, Ordering::SeqCst);


            }
        }
    }

    Ok(())
}

pub async fn download_single(
    id: u64,
    url: &str,
    output_path: &Path,
    downloaded: Arc<AtomicU64>,
    progress_tx: mpsc::Sender<DownloadEvent>,
    file_size: u64,
    cancel_token: CancellationToken,
) -> Result<()> {
    println!("single download started");
    let total = file_size;
    let progress_tx_clone = progress_tx.clone();
    let downloaded_clone = downloaded.clone();
    let cancel = cancel_token.clone();
    tokio::spawn(async move {
        // let mut last_downloaded = 0u64;
        // let mut last_time = std::time::Instant::now();
        loop {
            tokio::select! {
                _ = cancel.cancelled()=>{break;}
                _ = tokio::time::sleep(Duration::from_millis(500)) => {
                    let current = downloaded_clone.load(Ordering::SeqCst);
                    // let now = std::time::Instant::now();
                    // let elapsed = now.duration_since(last_time).as_secs_f64();
                    // let speed = if elapsed > 0.0 {
                    //     (current - last_downloaded) as f64 / elapsed
                    // } else {
                    //     0.0
                    // };

                    let _ = progress_tx_clone
                        .send(DownloadEvent::Progress {
                            id,
                            downloaded: current,
                            total,
                        })
                        .await;
                    // last_downloaded = current;
                    // last_time = now;
                    if current >= total {
                        break;
                    }
                }
            }
        }
    });

    let client = Client::new();
    let mut response = client.get(url).send().await?;

    println!("single response status: {}", response.status());

    let mut file = tokio::fs::File::create(output_path).await?;
    let mut total = 0u64;

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                break;
            }

            chunk = response.chunk() => {
                let chunk = match chunk? {
                    Some(chunk) => chunk,
                    None => break,
                };

                file.write_all(&chunk).await?;

                total += chunk.len() as u64;

                downloaded.fetch_add(
                    chunk.len() as u64,
                    Ordering::SeqCst,
                );

                println!("downloaded {} bytes", total);
            }
        }
    }

    println!("single download finished: {} bytes", total);
    Ok(())
}
