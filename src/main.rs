use hoptix::{
    config::{get_config_path, AppConf},
    models::{
        file_prop::prepare_file,
        message::{Command, Response},
    },
    routes::netcard,
    service::download::*,
    SockConnect,
};
use std::{
    io,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    net::{UnixListener, UnixStream},
    sync::{mpsc, Mutex},
    task::JoinHandle,
};

#[derive(Default)]
pub struct AppState {
    is_runing: bool,
    total: u64,
    downloaded: Arc<std::sync::atomic::AtomicU64>,
    start_time: Option<Instant>,
    handle: Option<JoinHandle<()>>,
    progress_tx: Option<mpsc::Sender<Response>>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            is_runing: false,
            total: 0,
            downloaded: Arc::new(AtomicU64::new(0)),
            start_time: None,
            handle: None,
            progress_tx: None,
        }
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let config = AppConf::load();
    println!("config loaded");

    let mut socket_path = get_config_path();
    socket_path.set_file_name("hoptix.sock");
    if socket_path.exists() {
        std::fs::remove_file(&socket_path).unwrap();
    }

    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let listener = UnixListener::bind(&socket_path).unwrap();
    println!("daemon started lisening on {}", socket_path.display());

    let state = Arc::new(Mutex::new(AppState::new()));
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let state_clone = state.clone();
        let conf_clone = Arc::new(config.clone());

        tokio::spawn(handle_client(stream, state_clone, conf_clone));
    }
}

async fn handle_client(mut stream: UnixStream, state: Arc<Mutex<AppState>>, config: Arc<AppConf>) {
    eprintln!("New client connected");
    let cmd = match SockConnect::read_command(&mut stream).await {
        Ok(cmd) => {
            eprintln!("Command recerived:{:?}", cmd);
            cmd
        }
        Err(e) => {
            eprintln!("Read command error:{}", e);
            let _ = SockConnect::send_response(
                &mut stream,
                &Response::Error(format!("Invalid command:{}", e)),
            );
            return;
        }
    };

    match cmd {
        Command::Start { url, output } => {
            eprintln!("Starting download for url:{}", url);
            let mut state_guard = state.lock().await;
            if state_guard.is_runing {
                let _ = SockConnect::send_response(
                    &mut stream,
                    &Response::Error("Download already in progress".to_string()),
                )
                .await;
                return;
            }

            let file_info = match inspect_url(&url).await {
                Ok(info) => {
                    eprintln!("URL inspected , size={}", info.size);
                    info
                }
                Err(e) => {
                    eprintln!("inspect url error:{e}");
                    let _ = SockConnect::send_response(
                        &mut stream,
                        &Response::Error(format!("Failed to inspect URL:{}", e)),
                    )
                    .await;
                    return;
                }
            };

            let output_path = if let Some(path) = output {
                PathBuf::from(path)
            } else {
                let filename = url.split('/').next_back().unwrap_or("download");
                config.download_dir.join(filename)
            };

            let final_path = match prepare_file(&output_path, file_info.size).await {
                Ok(p) => {
                    eprintln!("File perpared at {p:?}");
                    p
                }
                Err(e) => {
                    eprintln!("prepare file error:{e}");
                    let _ = SockConnect::send_response(
                        &mut stream,
                        &Response::Error(format!("Failed to prepare file:{}", e)),
                    )
                    .await;
                    return;
                }
            };
            let interface = match netcard::get_working_interface(&url).await {
                Ok(ifaces) => {
                    eprintln!("get_working_interface {}", ifaces.len());
                    ifaces
                }
                Err(e) => {
                    eprintln!("get_working_interface error:{e}");
                    let _ = SockConnect::send_response(
                        &mut stream,
                        &Response::Error(format!("No working interface:{e}")),
                    )
                    .await;
                    return;
                }
            };

            let (progress_tx, mut progress_rx) = mpsc::channel::<Response>(32);
            let downloaded = Arc::new(AtomicU64::new(0));

            let url_clone = url.clone();
            let path_clone = final_path.clone();
            let interface_clone = interface.clone();
            let downloaded_clone = downloaded.clone();
            let progress_tx_clone = progress_tx.clone();

            eprintln!("Spwning download task...");

            let handle = tokio::spawn(async move {
                if let Err(e) = download_file_parallel(
                    &url_clone,
                    file_info.size,
                    interface_clone,
                    &path_clone,
                    progress_tx_clone,
                    downloaded_clone,
                    config.chunk,
                    config.thread,
                )
                .await
                {
                    eprintln!("Download error: {e:?}");
                    let _ = progress_tx
                        .send(Response::Error(format!("Download failed: {e}")))
                        .await;
                }
            });

            state_guard.is_runing = true;
            state_guard.total = file_info.size;
            state_guard.downloaded = downloaded.clone();
            state_guard.start_time = Some(Instant::now());
            state_guard.handle = Some(handle);

            let id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            let _ = SockConnect::send_response(&mut stream, &Response::Started { id }).await;
            let result = tokio::time::timeout(Duration::from_secs(60), async {
                while let Some(progress) = progress_rx.recv().await {
                    let _ = SockConnect::send_response(&mut stream, &progress).await;
                    if matches!(progress, Response::Finished)
                        || matches!(progress, Response::Error(_))
                    {
                        break;
                    }
                }
            })
            .await;

            if result.is_err() {
                eprintln!("Progress timeout: no update recerived for 60 seconds");
            }

            // let mut state_guard = state.lock().await;
            // state_guard.is_runing = false;
            // state_guard.handle = None;
        }
        Command::Status => {
            let state_guard = state.lock().await;
            if !state_guard.is_runing {
                let _ = SockConnect::send_response(
                    &mut stream,
                    &Response::Error("No active download".to_string()),
                )
                .await;
            }

            let downloaded = state_guard.downloaded.load(Ordering::SeqCst);
            let total = state_guard.total;
            let speed = if let Some(start) = state_guard.start_time {
                let elapsed = start.elapsed().as_secs_f64();
                if elapsed > 0.0 {
                    downloaded as f64 / elapsed
                } else {
                    0.0
                }
            } else {
                0.0
            };
            let _ = SockConnect::send_response(
                &mut stream,
                &Response::Progress {
                    total,
                    downloaded,
                    speed,
                },
            )
            .await;
        }
        Command::Stop => {
            let mut state_guard = state.lock().await;
            if !state_guard.is_runing {
                let _ = SockConnect::send_response(
                    &mut stream,
                    &Response::Error("Not active download".to_string()),
                )
                .await;
            }
            if let Some(handle) = state_guard.handle.take() {
                handle.abort();
            }
            state_guard.is_runing = false;
            let _ = SockConnect::send_response(
                &mut stream,
                &Response::Error("Download stopped".to_string()),
            )
            .await;
        }
        Command::Pause | Command::Resume => {
            let _ = SockConnect::send_response(
                &mut stream,
                &Response::Error("Pause/Resume not implemented".to_string()),
            )
            .await;
        }
    }
}
