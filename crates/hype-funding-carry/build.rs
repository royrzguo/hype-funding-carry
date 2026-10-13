use std::path::{Path, PathBuf};
use std::process::Command;

// The audit trail records which code produced a decision. Reading HEAD at runtime answered
// with the working tree instead of the running binary, so a pull that had not been restarted
// yet made every row claim the new commit. Bake it in at compile time.
fn main() {
    let sha = describe_head().unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=GIT_SHA={sha}");
    for path in watched_paths() {
        // Printing a path that does not exist makes cargo re-run this script every build,
        // which is the safe direction: a stale sha is worse than a redundant rebuild.
        println!("cargo:rerun-if-changed={}", path.display());
    }
    println!("cargo:rerun-if-env-changed=GIT_SHA");
}

fn describe_head() -> Option<String> {
    if let Ok(preset) = std::env::var("GIT_SHA") {
        if !preset.trim().is_empty() {
            return Some(preset.trim().to_string());
        }
    }
    let sha = git(&["rev-parse", "HEAD"])?;
    // A dirty tree means the binary does not correspond to the commit it names; mark it.
    match git(&["status", "--porcelain"]) {
        Some(status) if !status.is_empty() => Some(format!("{sha}-dirty")),
        _ => Some(sha),
    }
}

fn git(args: &[&str]) -> Option<String> {
    let repo = repo_root();
    let mut command = Command::new("git");
    // Git refuses to read a checkout owned by another user without this, and the sha would
    // silently bake in as "unknown".
    if let Some(repo) = &repo {
        command.args(["-c", &format!("safe.directory={}", repo.display())]);
        command.current_dir(repo);
    }
    let output = command.args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8(output.stdout).ok()?.trim().to_string())
}

fn repo_root() -> Option<PathBuf> {
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").ok()?);
    manifest.ancestors().nth(2).map(Path::to_path_buf)
}

fn watched_paths() -> Vec<PathBuf> {
    let Some(repo) = repo_root() else {
        return Vec::new();
    };
    let git_dir = repo.join(".git");
    let head = git_dir.join("HEAD");
    let mut paths = vec![head.clone(), git_dir.join("packed-refs")];
    // On a branch, HEAD only names the ref; the sha lives in the ref file until `git gc`
    // moves it into packed-refs, so both have to be watched.
    if let Ok(contents) = std::fs::read_to_string(&head) {
        if let Some(reference) = contents.trim().strip_prefix("ref: ") {
            paths.push(git_dir.join(reference));
        }
    }
    paths
}
