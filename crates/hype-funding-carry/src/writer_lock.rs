use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use fs2::FileExt;
use uuid::Uuid;

pub struct WriterLock {
    file: File,
}

impl WriterLock {
    pub fn acquire(account: &str) -> Result<Self> {
        let directory = lock_directory(crate::state_io::state_path("locks"))?;
        Self::acquire_path(directory.join(lock_name(account)))
    }

    fn acquire_path(path: PathBuf) -> Result<Self> {
        let token = format!("{}:{}", std::process::id(), Uuid::new_v4());
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .context("open HYPE Funding Carry writer lock")?;
        if file.try_lock_exclusive().is_err() {
            bail!(
                "another HYPE Funding Carry live writer holds {}",
                path.display()
            );
        }
        file.set_len(0)
            .context("truncate HYPE Funding Carry writer lock")?;
        writeln!(file, "{token}").context("write HYPE Funding Carry writer lock")?;
        file.sync_all()
            .context("sync HYPE Funding Carry writer lock")?;
        Ok(Self { file })
    }
}

fn lock_name(account: &str) -> String {
    format!(
        "hfc-{}.lock",
        account.trim_start_matches("0x").to_ascii_lowercase()
    )
}

fn lock_directory(directory: PathBuf) -> Result<PathBuf> {
    std::fs::create_dir_all(&directory).context("create writer lock directory")?;
    directory
        .canonicalize()
        .context("resolve writer lock directory")
}

impl Drop for WriterLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_one_writer_can_hold_an_account_lock() {
        let path =
            std::env::temp_dir().join(format!("hype-funding-carry-writer-lock-{}", Uuid::new_v4()));
        let first = WriterLock::acquire_path(path.clone()).unwrap();
        assert!(WriterLock::acquire_path(path.clone()).is_err());
        drop(first);
        let second = WriterLock::acquire_path(path.clone()).unwrap();
        drop(second);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn lock_directory_creates_a_local_directory() {
        let missing =
            std::env::temp_dir().join(format!("hype-funding-carry-locks-{}", Uuid::new_v4()));
        assert_eq!(
            lock_directory(missing.clone()).unwrap(),
            missing.canonicalize().unwrap()
        );
        std::fs::remove_dir(missing).unwrap();
    }

    #[test]
    fn lock_name_is_stable_per_account() {
        assert_eq!(lock_name("0xAbCd"), "hfc-abcd.lock");
    }
}
