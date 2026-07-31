use crate::error::{AppError, Result};
use get_if_addrs::get_if_addrs;
use reqwest::Client;
use std::net::IpAddr;
use std::time::Duration;
use tokio::task::JoinSet;

#[derive(Debug, Clone)]
pub struct NetworkInterface {
    pub name: String,
    pub ip: IpAddr,
}

pub fn get_available_interface() -> Result<Vec<NetworkInterface>> {
    let interfaces = get_if_addrs().map_err(AppError::Io)?;

    let valid_iterface = interfaces
        .into_iter()
        .filter(|iface| !iface.is_loopback())
        .map(|iface| NetworkInterface {
            name: iface.name,
            ip: iface.addr.ip(),
        })
        .collect();
    Ok(valid_iterface)
}

pub async fn check_interface_connectivity(ip: IpAddr, url: &str) -> bool {
    let client = match Client::builder()
        .local_address(ip)
        .timeout(Duration::from_secs(3))
        .build()
    {
        Ok(c) => c,
        Err(_) => return false,
    };

    match client.head(url).send().await {
        Ok(response) => response.status().is_success() || response.status().is_redirection(),
        Err(_) => false,
    }
}

pub async fn get_working_interface(target_url: &str) -> Result<Vec<NetworkInterface>> {
    let all_interfaces = get_available_interface()?;
    let mut join_set = JoinSet::new();
    let mut working = Vec::new();

    for iface in all_interfaces {
        let url = target_url.to_string();

        join_set.spawn(async move {
            let is_ok = check_interface_connectivity(iface.ip, &url).await;
            (iface, is_ok)
        });

        while let Some(res) = join_set.join_next().await {
            if let Ok((iface, true)) = res {
                working.push(iface);
            }
        }
    }

    if working.is_empty() {
        return Err(AppError::NetcardNotFound(
            "cant find any network card canncted to network".to_string(),
        ));
    }
    Ok(working)
}
