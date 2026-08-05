use clap::Parser;
use hoptix::config::get_config_path;
use hoptix::models::message::{Command, Response};
use hoptix::SockConnect;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::net::UnixStream;
use tokio::time::{sleep, Duration};

#[derive(Parser)]
#[command(name = "hoptix")]
#[command(about = "Hoptix download manager CLI")]
enum Cli {
    Start {
        url: String,
        #[arg(short, long)]
        output: Option<String>,
    },
    Status,
    Stop,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();
    let mut socket_path = get_config_path();
    socket_path.set_file_name("hoptix.sock");
    let mut stream = UnixStream::connect(&socket_path).await?;

    match args {
        Cli::Start { url, output } => {
            let cmd = Command::Start { url, output };
            SockConnect::send_command(&mut stream, &cmd).await?;

            let response = SockConnect::read_response(&mut stream).await?;
            match response {
                Response::Started { id } => {
                    println!("download by id  {} started", id);
                    show_progress(&mut stream).await?;
                }
                Response::Error(e) => {
                    eprintln!("daemon Error: {}", e);
                }
                _ => {
                    eprintln!("resp invalid from daemon");
                }
            }
        }
        Cli::Status => {
            let cmd = Command::Status;
            SockConnect::send_command(&mut stream, &cmd).await?;
            let response = SockConnect::read_response(&mut stream).await?;
            match response {
                Response::Progress {
                    total,
                    downloaded,
                    speed,
                } => {
                    println!(
                        "process: {}/{} bytes, speed: {:.2} MB/s",
                        downloaded,
                        total,
                        speed / 1_000_000.0
                    );
                }
                Response::Finished => {
                    println!("download completed");
                }
                Response::Error(e) => {
                    eprintln!("Error: {}", e);
                }
                _ => {}
            }
        }
        Cli::Stop => {
            let cmd = Command::Stop;
            SockConnect::send_command(&mut stream, &cmd).await?;
            println!("send pause signal");
        }
    }

    Ok(())
}

async fn show_progress(stream: &mut UnixStream) -> Result<(), Box<dyn std::error::Error>> {
    let pb = ProgressBar::new(0);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );

    loop {
        let response = SockConnect::read_response(stream).await?;
        match response {
            Response::Progress {
                total,
                downloaded,
                speed,
            } => {
                if pb.length().is_none() {
                    pb.set_length(total);
                }
                pb.set_position(downloaded);
                pb.set_message(format!("{:.2} MB/s", speed / 1_000_000.0));
            }
            Response::Finished => {
                pb.finish_with_message("download completed");
                break;
            }
            Response::Error(e) => {
                pb.finish_with_message(format!("Error: {}", e));
                break;
            }
            _ => {}
        }
        sleep(Duration::from_millis(100)).await;
    }

    Ok(())
}
