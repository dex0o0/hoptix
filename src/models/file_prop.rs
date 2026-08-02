use crate::error::{AppError, Result};
use std::path::{Path, PathBuf};
use tokio::fs::{self, File};

pub async fn resolve_path(target_path: impl AsRef<Path>) -> PathBuf {
    let path = target_path.as_ref();
    if !path.exists() {
        return path.to_path_buf();
    }

    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("download");

    let extension = path
        .extension()
        .and_then(|s| s.to_str())
        .map(|e| format!(".{e}"))
        .unwrap_or_default();

    let parent = path.parent().unwrap_or_else(|| Path::new(""));

    let mut count = 1;
    loop {
        let new_filename = format!("{} ({}){}", stem, count, extension);
        let new_path = parent.join(new_filename);
        if !new_path.exists() {
            return new_path;
        }
        count += 1;
    }
}

pub async fn prepare_file(target_path: impl AsRef<Path>, file_size: u64) -> Result<PathBuf> {
    let final_path = resolve_path(target_path).await;

    let file = File::create(&final_path).await.map_err(AppError::Io)?;
    file.set_len(file_size).await.map_err(AppError::Io)?;

    Ok(final_path)
}
