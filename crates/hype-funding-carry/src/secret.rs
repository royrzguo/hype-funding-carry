use std::path::PathBuf;
use std::sync::OnceLock;

use anyhow::{bail, Context, Result};

// Load an API-agent key locally. Never accept a master key through chat or CLI.
pub fn load_private_key() -> Result<String> {
    static PRIVATE_KEY: OnceLock<String> = OnceLock::new();
    if let Some(value) = PRIVATE_KEY.get() {
        return Ok(value.clone());
    }
    let path = PathBuf::from(
        std::env::var_os("HFC_PRIVATE_KEY_FILE").context("HFC_PRIVATE_KEY_FILE is required")?,
    );
    let metadata = std::fs::metadata(&path).context("inspect agent key file")?;
    if !metadata.is_file() {
        bail!("agent key must be a regular file");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!("agent key file must not be accessible to group or other users");
        }
    }
    let value = std::fs::read_to_string(path).context("read local agent key")?;
    let value = value.trim().to_string();
    if value.is_empty() {
        bail!("agent key file is empty");
    }
    let _ = PRIVATE_KEY.set(value.clone());
    Ok(value)
}
