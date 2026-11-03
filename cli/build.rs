//! Stamp the binary with a sub-version: `CW_GIT_SHA` always, `CW_BUILD_TAG`
//! when the package script sets `CLIPBOARDWIRE_BUILD_TAG=lazyN` so ad-hoc test
//! builds are distinguishable from real releases at a glance.

fn main() {
    println!("cargo:rerun-if-env-changed=CLIPBOARDWIRE_BUILD_TAG");
    if let Ok(tag) = std::env::var("CLIPBOARDWIRE_BUILD_TAG") {
        println!("cargo:rustc-env=CW_BUILD_TAG={tag}");
    }

    let sha = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".into());
    let dirty = std::process::Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .map(|o| !o.stdout.is_empty())
        .unwrap_or(false);
    println!(
        "cargo:rustc-env=CW_GIT_SHA={}{}",
        sha,
        if dirty { "-dirty" } else { "" }
    );
}
