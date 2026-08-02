use crate::error::{AppError, Result};
use crate::models::request::{check_range_support, Chunck, FileInfo};
use crate::routes::netcard::NetworkInterface;
use futures::StreamExt;
use reqwest::Client;
use std::{collections::VecDeque, net::IpAddr, path::Path, sync::Arc, time::Duration};
use tokio::io::BufWriter;
use tokio::{
    fs::OpenOptions,
    io::{AsyncSeekExt, AsyncWriteExt, SeekFrom},
    sync::Mutex,
    task::JoinSet,
};

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

pub fn create_chunck(file_size: u64, num_chunck: usize) -> Vec<Chunck> {
    const MIN_CHUNCK_SIZE: u64 = 2 * 1024 * 1024;

    let optimal = (file_size / MIN_CHUNCK_SIZE) as usize;
    let num_chunck = optimal.clamp(1, num_chunck);

    let mut chuncks = Vec::new();

    if num_chunck == 0 || file_size == 0 {
        return chuncks;
    }
    let num_chunck_u64 = num_chunck as u64;
    let base_chunck_size = file_size / num_chunck_u64;

    for i in 0..num_chunck {
        let start = (i as u64) * base_chunck_size;

        let end = if i == num_chunck - 1 {
            file_size - 1
        } else {
            ((i as u64) + 1) * base_chunck_size - 1
        };

        chuncks.push(Chunck {
            id: i,
            start,
            end,
            downloaded_bytes: 0,
            assigned_ip: None,
        });
    }
    chuncks
}

pub async fn download_chunck(
    url: &str,
    chunck: Chunck,
    bound_ip: IpAddr,
    file_path: &Path,
) -> Result<()> {
    let client = Client::builder()
        .local_address(bound_ip)
        .connect_timeout(Duration::from_secs(5))
        .read_timeout(Duration::from_secs(15))
        .pool_max_idle_per_host(10)
        .build()?;

    let range_header = format!("bytes={}-{}", chunck.start, chunck.end);
    let mut response = client.get(url).header("Range", range_header).send().await?;

    if !response.status().is_success() {
        return Err(AppError::RangeNotSupported);
    }

    let mut file = OpenOptions::new()
        .write(true)
        .open(file_path)
        .await
        .map_err(AppError::Io)?;

    file.seek(SeekFrom::Start(chunck.start))
        .await
        .map_err(AppError::Io)?;

    while let Some(data_chunck) = response.chunk().await? {
        file.write_all(&data_chunck).await.map_err(AppError::Io)?;
    }

    Ok(())
}

const CONC_PER_IFACE: usize = 16;

pub async fn download_file_parallel(
    url: &str,
    file_size: u64,
    interface: Vec<NetworkInterface>,
    output_path: &Path,
) -> Result<()> {
    let chuncks = create_chunck(file_size, 16);
    let chunck_queue = Arc::new(Mutex::new(VecDeque::from(chuncks)));

    let mut joinset = JoinSet::new();
    for iface in interface {
        for thread_id in 0..CONC_PER_IFACE {
            let queue = Arc::clone(&chunck_queue);
            let url = url.to_string();
            let path = output_path.to_path_buf();
            let iface = iface.clone();
            joinset.spawn(async move {
                let client = match Client::builder()
                    .interface(&iface.name)
                    .connect_timeout(Duration::from_secs(5))
                    .read_timeout(Duration::from_secs(15))
                    .pool_max_idle_per_host(10)
                    .build()
                {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!(
                            "can not create client for [{}] (message::Error::{e})",
                            iface.name
                        );
                        return;
                    }
                };

                let mut faile_count = 0;
                const MX_FAILS: u32 = 3;
                loop {
                    if faile_count >= MX_FAILS {
                        break;
                    }
                    let maybe_chunck = {
                        let mut lock = queue.lock().await;
                        lock.pop_front()
                    };

                    let chunck = match maybe_chunck {
                        Some(c) => c,
                        None => break,
                    };

                    println!(
                        "[{}] downloading {} (bytes={}-{})",
                        iface.name, chunck.id, chunck.start, chunck.end
                    );

                    if let Err(e) =
                        download_chunck_with_client(&client, &url, chunck.clone(), &path).await
                    {
                        faile_count += 1;
                        eprintln!(
                            "Error on [{}] to download {}: (error message:{:?})",
                            iface.name, chunck.id, e
                        );
                        let mut lock = queue.lock().await;
                        lock.push_back(chunck);
                        drop(lock);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    } else {
                        faile_count = 0;
                        println!("chonck:{} downloading with [{}]", chunck.id, iface.name);
                    }
                    println!();
                }
            });
        }
    }

    while let Some(res) = joinset.join_next().await {
        res.map_err(|e| AppError::IpcError(e.to_string()))?;
    }

    Ok(())
}

pub async fn download_chunck_with_client(
    client: &Client,
    url: &str,
    chunck: Chunck,
    file_path: &Path,
) -> Result<()> {
    let range_header = format!("bytes={}-{}", chunck.start, chunck.end);
    let response = client.get(url).header("Range", range_header).send().await?;

    if !response.status().is_success() {
        return Err(AppError::RangeNotSupported);
    }

    let mut file = OpenOptions::new()
        .write(true)
        .open(file_path)
        .await
        .map_err(AppError::Io)?;

    file.seek(SeekFrom::Start(chunck.start))
        .await
        .map_err(AppError::Io)?;

    let mut writer = BufWriter::with_capacity(128 * 1024, file);
    let mut stream = response.bytes_stream();

    while let Some(item) = stream.next().await {
        let data = item?;
        writer.write_all(&data).await.map_err(AppError::Io)?;
    }

    writer.flush().await.map_err(AppError::Io)?;

    Ok(())
}
