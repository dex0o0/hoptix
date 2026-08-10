use hoptix::{
    AppState, SockConnect,
    config::{AppConf, get_config_path},
    error::Result,
    handle::cmd_handle::CommandHandle,
    models::{
        file_prop::prepare_file,
        message::{Command, Response},
        request::is_localhost,
    },
    routes::netcard,
    service::{
        download::*,
        download_task::DownloadTask,
        manager::{DownloadManager, DownloadSession},
    },
};
use std::{
    clone, io,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    net::{UnixListener, UnixStream},
    sync::{Mutex, mpsc},
};

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

    let listener = UnixListener::bind(&socket_path)?;
    println!("daemon started lisening on {}", socket_path.display());

    let (event_tx, mut event_rx) = mpsc::channel(100);

    let state = Arc::new(Mutex::new(AppState::new(event_tx)));

    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            println!("EVENT:{:?}", event);
        }
    });

    let config = Arc::new(config);

    loop {
        let (stream, _) = listener.accept().await?;

        let state = Arc::clone(&state);
        let config = Arc::clone(&config);

        tokio::spawn(async move {
            handle_client(stream, state, config).await;
        });
    }
}

async fn handle_client(stream: UnixStream, state: Arc<Mutex<AppState>>, config: Arc<AppConf>) {
    eprintln!("New client connected");

    let stream = Arc::new(Mutex::new(stream));

    let command = {
        let mut stream_guard = stream.lock().await;

        match SockConnect::read_command(&mut stream_guard).await {
            Ok(cmd) => {
                eprintln!("Command received: {:?}", cmd);
                cmd
            }

            Err(e) => {
                eprintln!("Read command error: {}", e);

                let _ = SockConnect::send_response(
                    &mut stream_guard,
                    &Response::Error(format!("Invalid command: {}", e)),
                )
                .await;

                return;
            }
        }
    };

    let handler = CommandHandle {
        state,
        config,
        stream,
    };

    if let Err(e) = handler.handle(command).await {
        eprintln!("Command error: {}", e);
    }
}
