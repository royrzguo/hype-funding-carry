use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub fn state_path(file_name: impl AsRef<Path>) -> PathBuf {
    std::env::var("HFC_STATE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("state"))
        .join(file_name)
}

pub fn durable_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).context("create HYPE Funding Carry state directory")?;
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = path.with_extension(format!("tmp.{}.{sequence}", std::process::id()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .context("create HYPE Funding Carry temporary state")?;
    file.write_all(bytes)
        .context("write HYPE Funding Carry temporary state")?;
    file.sync_all()
        .context("sync HYPE Funding Carry temporary state")?;
    fs::rename(&temporary, path).context("activate HYPE Funding Carry state")?;
    sync_directory(parent)
}

pub fn durable_remove(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => sync_directory(path.parent().unwrap_or_else(|| Path::new("."))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).context("remove HYPE Funding Carry state"),
    }
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        fs::File::open(path)
            .context("open HYPE Funding Carry state directory")?
            .sync_all()
            .context("sync HYPE Funding Carry state directory")
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}
