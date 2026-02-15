//! Build slipstream-picoquic: locate or build the C library, then link.
//! Same approach as slipstream-rust: use PICOQUIC_* env vars and optional build script.
//!
//! - PICOQUIC_DIR: source dir (default: ../slipstream-picoquic from this crate).
//! - PICOQUIC_BUILD_DIR: cmake build output (default: ../.slipstream-picoquic-build from repo root).
//! - PICOQUIC_INCLUDE_DIR / PICOQUIC_LIB_DIR: override include or lib location.
//! - PICOQUIC_AUTO_BUILD=1: run scripts/build_slipstream_picoquic.sh if libs missing.

use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-env-changed=PICOQUIC_DIR");
    println!("cargo:rerun-if-env-changed=PICOQUIC_BUILD_DIR");
    println!("cargo:rerun-if-env-changed=PICOQUIC_INCLUDE_DIR");
    println!("cargo:rerun-if-env-changed=PICOQUIC_LIB_DIR");
    println!("cargo:rerun-if-env-changed=PICOQUIC_AUTO_BUILD");
    println!("cargo:rerun-if-env-changed=OPENSSL_ROOT_DIR");

    let target = env::var("TARGET").unwrap_or_default();
    let is_windows = target.contains("windows") || target.contains("pc-windows");
    let auto_build = env_flag("PICOQUIC_AUTO_BUILD", true);
    let explicit_include = env::var_os("PICOQUIC_INCLUDE_DIR").is_some();
    let explicit_lib = env::var_os("PICOQUIC_LIB_DIR").is_some();
    let explicit_include_lib = explicit_include || explicit_lib;

    let mut picoquic_include_dir = locate_picoquic_include_dir();
    let mut picoquic_lib_dir = locate_picoquic_lib_dir();

    if auto_build && !explicit_include_lib && (picoquic_include_dir.is_none() || picoquic_lib_dir.is_none()) {
        build_picoquic(&target)?;
        picoquic_include_dir = locate_picoquic_include_dir();
        picoquic_lib_dir = locate_picoquic_lib_dir();
    }

    if explicit_include_lib {
        if picoquic_include_dir.is_none() {
            return Err("PICOQUIC_INCLUDE_DIR or PICOQUIC_LIB_DIR set but headers not found; set PICOQUIC_INCLUDE_DIR.".into());
        }
        if picoquic_lib_dir.is_none() {
            return Err("PICOQUIC_INCLUDE_DIR or PICOQUIC_LIB_DIR set but libs not found; set PICOQUIC_LIB_DIR.".into());
        }
    }

    let _picoquic_include_dir = picoquic_include_dir.ok_or_else(|| {
        "Missing slipstream-picoquic headers. Add submodule: git submodule update --init --recursive. \
         Then run: ./scripts/build_slipstream_picoquic.sh (or bash scripts/build_slipstream_picoquic.sh on Windows). \
         Or set PICOQUIC_DIR / PICOQUIC_INCLUDE_DIR."
    })?;
    let picoquic_lib_dir = picoquic_lib_dir.ok_or_else(|| {
        "Missing slipstream-picoquic libs. Run ./scripts/build_slipstream_picoquic.sh or set PICOQUIC_BUILD_DIR / PICOQUIC_LIB_DIR."
    })?;

    let picoquic_libs = resolve_picoquic_libs(&picoquic_lib_dir).ok_or_else(|| {
        "Could not find required static libs (picoquic-core, picotls-*) in build dir."
    })?;

    for dir in &picoquic_libs.search_dirs {
        println!("cargo:rustc-link-search=native={}", dir.display());
    }
    for lib in &picoquic_libs.libs {
        println!("cargo:rustc-link-lib=static={}", lib);
    }

    if is_windows {
        println!("cargo:rustc-link-lib=dylib=libssl");
        println!("cargo:rustc-link-lib=dylib=libcrypto");
        if let Ok(dir) = env::var("OPENSSL_LIB_DIR") {
            println!("cargo:rustc-link-search=native={}", dir);
        }
        println!("cargo:rustc-link-lib=dylib=ws2_32");
        println!("cargo:rustc-link-lib=dylib=bcrypt");
    } else {
        println!("cargo:rustc-link-lib=dylib=ssl");
        println!("cargo:rustc-link-lib=dylib=crypto");
        println!("cargo:rustc-link-lib=dylib=pthread");
    }

    Ok(())
}

fn env_flag(key: &str, default: bool) -> bool {
    match env::var(key) {
        Ok(v) => {
            let v = v.trim();
            matches!(v.to_lowercase().as_str(), "1" | "true" | "yes" | "on")
        }
        Err(_) => default,
    }
}

fn locate_repo_root() -> Option<PathBuf> {
    let manifest = env::var("CARGO_MANIFEST_DIR").ok()?;
    let crate_dir = Path::new(&manifest);
    // slipstream_picoquic_sys is at repo/slipstream_picoquic_sys, so parent = repo
    crate_dir.parent().map(Path::to_path_buf)
}

fn build_picoquic(target: &str) -> Result<(), Box<dyn std::error::Error>> {
    let root = locate_repo_root().ok_or("Could not locate repo root for build script")?;
    let script = root.join("scripts").join("build_slipstream_picoquic.sh");
    if !script.exists() {
        return Err(format!("Build script not found: {}. Run git submodule update --init.", script.display()).into());
    }
    let picoquic_dir = env::var_os("PICOQUIC_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join("slipstream-picoquic"));
    let build_dir = env::var_os("PICOQUIC_BUILD_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join(".slipstream-picoquic-build"));

    let mut cmd = if cfg!(target_os = "windows") {
        let git_bash = [
            PathBuf::from(r"C:\Program Files\Git\bin\bash.exe"),
            PathBuf::from(r"C:\Program Files\Git\usr\bin\bash.exe"),
        ];
        let bash = git_bash.iter().find(|p| p.exists())
            .ok_or("Git Bash not found. Install Git for Windows or build picoquic manually.")?;
        let mut c = Command::new(bash);
        c.arg(&script);
        c
    } else {
        Command::new(&script)
    };

    cmd.env("PICOQUIC_DIR", &picoquic_dir)
        .env("PICOQUIC_BUILD_DIR", &build_dir)
        .env("PICOQUIC_TARGET", target);
    if let Ok(v) = env::var("OPENSSL_ROOT_DIR") {
        cmd.env("OPENSSL_ROOT_DIR", v);
    }
    let status = cmd.status()?;
    if !status.success() {
        return Err("slipstream-picoquic build script failed. Run scripts/build_slipstream_picoquic.sh for details.".into());
    }
    Ok(())
}

fn locate_picoquic_include_dir() -> Option<PathBuf> {
    if let Ok(dir) = env::var("PICOQUIC_INCLUDE_DIR") {
        let p = PathBuf::from(&dir);
        if p.join("picoquic.h").exists() {
            return Some(p);
        }
        let p2 = Path::new(&dir).join("picoquic");
        if p2.join("picoquic.h").exists() {
            return Some(p2);
        }
    }
    if let Ok(dir) = env::var("PICOQUIC_DIR") {
        let p = Path::new(&dir).join("picoquic");
        if p.join("picoquic.h").exists() {
            return Some(p);
        }
    }
    if let Some(root) = locate_repo_root() {
        let p = root.join("slipstream-picoquic").join("picoquic");
        if p.join("picoquic.h").exists() {
            return Some(p);
        }
    }
    let manifest = env::var("CARGO_MANIFEST_DIR").ok()?;
    let p = Path::new(&manifest).join("..").join("slipstream-picoquic").join("picoquic");
    if p.join("picoquic.h").exists() {
        return Some(p.canonicalize().unwrap_or(p));
    }
    None
}

fn locate_picoquic_lib_dir() -> Option<PathBuf> {
    if let Ok(dir) = env::var("PICOQUIC_LIB_DIR") {
        let p = PathBuf::from(dir);
        if resolve_picoquic_libs(&p).is_some() {
            return Some(p);
        }
    }
    if let Ok(dir) = env::var("PICOQUIC_BUILD_DIR") {
        let p = PathBuf::from(dir);
        if resolve_picoquic_libs(&p).is_some() {
            return Some(p);
        }
    }
    if let Some(root) = locate_repo_root() {
        let p = root.join(".slipstream-picoquic-build");
        if resolve_picoquic_libs(&p).is_some() {
            return Some(p);
        }
    }
    None
}

struct PicoquicLibs {
    search_dirs: Vec<PathBuf>,
    libs: Vec<String>,
}

fn resolve_picoquic_libs(dir: &Path) -> Option<PicoquicLibs> {
    if let Some(libs) = resolve_picoquic_libs_single(dir) {
        return Some(PicoquicLibs {
            search_dirs: vec![dir.to_path_buf()],
            libs,
        });
    }
    let ptls_dirs = [
        dir.join("_deps").join("picotls-build"),
        dir.join("_deps").join("picotls-build").join("Release"),
        dir.join("_deps").join("picotls-build").join("Debug"),
    ];
    for ptls_dir in &ptls_dirs {
        if let Some(libs) = resolve_picoquic_libs_split(dir, ptls_dir) {
            let mut search_dirs = vec![dir.to_path_buf()];
            if ptls_dir != dir && !search_dirs.contains(&ptls_dir.to_path_buf()) {
                search_dirs.push(ptls_dir.to_path_buf());
            }
            return Some(PicoquicLibs { search_dirs, libs });
        }
    }
    None
}

fn resolve_picoquic_libs_single(dir: &Path) -> Option<Vec<String>> {
    const NAMES: [&str; 5] = ["picoquic-core", "picotls-core", "picotls-fusion", "picotls-openssl", "picotls-minicrypto"];
    let mut libs = Vec::with_capacity(NAMES.len());
    for name in NAMES {
        let underscored = name.replace('-', "_");
        if dir.join(format!("lib{}.a", name)).exists() || dir.join(format!("{}.lib", name)).exists() {
            libs.push(name.to_string());
        } else if dir.join(format!("lib{}.a", underscored)).exists() || dir.join(format!("{}.lib", underscored)).exists() {
            libs.push(underscored);
        } else {
            return None;
        }
    }
    Some(libs)
}

fn resolve_picoquic_libs_split(picoquic_dir: &Path, picotls_dir: &Path) -> Option<Vec<String>> {
    let has = |d: &Path, name: &str| {
        let u = name.replace('-', "_");
        d.join(format!("lib{}.a", name)).exists() || d.join(format!("{}.lib", name)).exists()
            || d.join(format!("lib{}.a", u)).exists() || d.join(format!("{}.lib", u)).exists()
    };
    if !has(picoquic_dir, "picoquic-core") {
        return None;
    }
    let ptls_names = ["picotls-core", "picotls-fusion", "picotls-openssl", "picotls-minicrypto"];
    for n in &ptls_names {
        if !has(picotls_dir, n) {
            return None;
        }
    }
    let mut libs = vec!["picoquic-core".to_string()];
    for n in &ptls_names {
        libs.push(n.to_string());
    }
    Some(libs)
}
