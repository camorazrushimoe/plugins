//! Config load, precedence and permission enforcement (§2).
//!
//! Precedence: CLI > env > config file > built-in defaults.
//! A **relative** `data_dir` resolves against the directory of the config file
//! that supplied it; if no config file was used, against the binary's
//! directory. A relative `WFDC_DATA_DIR` env value resolves against the
//! binary's directory. Never against the process cwd.

use std::path::{Path, PathBuf};

use crate::Error;

pub const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1:6380";
pub const DEFAULT_STREAM: &str = "office:events";
pub const DEFAULT_DATA_DIR: &str = "./wfdc-data";
pub const DEFAULT_MAX_MB: u64 = 500;
pub const DEFAULT_EXPIRE_HOURS: u64 = 6;

/// Resolved runtime configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub redis_url: String,
    pub stream: String,
    pub data_dir: PathBuf,
    /// Hard cap on collected JSONL under data_dir (MB). Normalized per §2:
    /// 0 → 500, 1–15 → 16.
    pub max_mb: u64,
    /// Expiry window in hours (§5.3). 0 → 6.
    pub expire_hours: u64,
}

/// Explicit CLI values (None = not given).
#[derive(Debug, Default, Clone)]
pub struct CliOverrides {
    pub config: Option<PathBuf>,
    pub redis_url: Option<String>,
    pub stream: Option<String>,
    pub max_mb: Option<u64>,
    pub expire_hours: Option<u64>,
}

fn binary_dir() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
        .unwrap_or_else(|| PathBuf::from("."))
}

fn config_search_path(cli: &CliOverrides) -> PathBuf {
    if let Some(p) = &cli.config {
        return p.clone();
    }
    if let Ok(p) = std::env::var("WFDC_CONFIG") {
        return PathBuf::from(p);
    }
    let bin = binary_dir().join("wfdc.toml");
    if bin.exists() {
        return bin;
    }
    PathBuf::from("wfdc.toml")
}

/// File-backed config values.
#[derive(Debug, Default, serde::Deserialize)]
struct FileConfig {
    redis_url: Option<String>,
    stream: Option<String>,
    data_dir: Option<String>,
    max_mb: Option<i64>,
}

/// Enforce wfdc.toml 0600 (§2): warn and exit 1 if group/world-readable.
fn check_config_perms(path: &Path) -> Result<(), Error> {
    use std::os::unix::fs::PermissionsExt;
    let meta = std::fs::metadata(path)
        .map_err(|e| Error::Fatal(format!("config {} unreadable: {e}", path.display())))?;
    let mode = meta.permissions().mode();
    if mode & 0o077 != 0 {
        return Err(Error::Fatal(format!(
            "config {} is group/world-readable (mode {:04o}); must be 0600 (holds the Redis password)",
            path.display(),
            mode & 0o777
        )));
    }
    Ok(())
}

/// Resolve the effective config. Order: CLI > env > file > defaults.
pub fn resolve(cli: &CliOverrides) -> Result<Config, Error> {
    // ---- defaults ----
    let mut redis_url = DEFAULT_REDIS_URL.to_string();
    let mut stream = DEFAULT_STREAM.to_string();
    let mut data_dir: PathBuf = PathBuf::from(DEFAULT_DATA_DIR);
    let mut max_mb: i64 = 0; // 0 → default 500
    let mut expire_hours: i64 = 0; // 0 → default 6

    // ---- config file ----
    let cfg_path = config_search_path(cli);
    let mut cfg_base: Option<PathBuf> = None;
    if cfg_path.exists() {
        check_config_perms(&cfg_path)?;
        let text = std::fs::read_to_string(&cfg_path)
            .map_err(|e| Error::Fatal(format!("config {}: {e}", cfg_path.display())))?;
        let file: FileConfig = toml::from_str(&text)
            .map_err(|e| Error::Fatal(format!("config {}: {e}", cfg_path.display())))?;
        if let Some(v) = file.redis_url {
            redis_url = v;
        }
        if let Some(v) = file.stream {
            stream = v;
        }
        if let Some(v) = file.data_dir {
            data_dir = PathBuf::from(v);
            cfg_base = cfg_path.parent().map(|d| d.to_path_buf());
        }
        if let Some(v) = file.max_mb {
            max_mb = v;
        }
    }

    // ---- env ----
    if let Ok(v) = std::env::var("WFDC_REDIS_URL") {
        if !v.is_empty() {
            redis_url = v;
        }
    }
    if let Ok(v) = std::env::var("WFDC_STREAM") {
        if !v.is_empty() {
            stream = v;
        }
    }
    if let Ok(v) = std::env::var("WFDC_DATA_DIR") {
        if !v.is_empty() {
            data_dir = PathBuf::from(v);
            cfg_base = None; // env has no config-file base; resolves vs binary dir
        }
    }
    if let Ok(v) = std::env::var("WFDC_MAX_MB") {
        if let Ok(n) = v.trim().parse::<i64>() {
            max_mb = n;
        }
    }
    if let Ok(v) = std::env::var("WFDC_EXPIRE_HOURS") {
        if let Ok(n) = v.trim().parse::<i64>() {
            expire_hours = n;
        }
    }

    // ---- CLI ----
    if let Some(v) = &cli.redis_url {
        redis_url = v.clone();
    }
    if let Some(v) = &cli.stream {
        stream = v.clone();
    }
    if let Some(v) = cli.max_mb {
        max_mb = v as i64;
    }
    if let Some(v) = cli.expire_hours {
        expire_hours = v as i64;
    }

    // ---- resolve relative data_dir ----
    let base = match &cfg_base {
        Some(b) => b.clone(),
        None => binary_dir(),
    };
    if data_dir.is_relative() {
        data_dir = base.join(&data_dir);
    }

    // ---- normalize max_mb (§2): 0/missing/negative → 500; 1–15 → 16 ----
    let max_mb = if max_mb <= 0 {
        DEFAULT_MAX_MB
    } else if (1..=15).contains(&max_mb) {
        16
    } else {
        max_mb as u64
    };

    let expire_hours = if expire_hours <= 0 {
        DEFAULT_EXPIRE_HOURS
    } else {
        expire_hours as u64
    };

    Ok(Config {
        redis_url,
        stream,
        data_dir,
        max_mb,
        expire_hours,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "wfdc-cfg-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn defaults_when_nothing_given() {
        // Clear env vars that other parallel tests may have set.
        for v in [
            "WFDC_REDIS_URL",
            "WFDC_STREAM",
            "WFDC_DATA_DIR",
            "WFDC_MAX_MB",
            "WFDC_EXPIRE_HOURS",
        ] {
            std::env::remove_var(v);
        }
        let c = resolve(&CliOverrides::default()).unwrap();
        assert_eq!(c.redis_url, DEFAULT_REDIS_URL);
        assert_eq!(c.stream, DEFAULT_STREAM);
        assert_eq!(c.max_mb, 500);
        assert_eq!(c.expire_hours, 6);
        // relative default resolves against the binary dir, not cwd
        assert!(c.data_dir.is_absolute());
    }

    #[test]
    fn cli_beats_env_beats_file() {
        use std::os::unix::fs::PermissionsExt;
        let d = tmpdir("prec");
        let p = d.join("wfdc.toml");
        std::fs::write(
            &p,
            "redis_url = \"redis://file:1\"\nstream = \"file:events\"\nmax_mb = 100\n",
        )
        .unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
        let cli = CliOverrides {
            config: Some(p),
            redis_url: Some("redis://cli:1".into()),
            stream: Some("cli:events".into()),
            max_mb: Some(1000),
            ..Default::default()
        };
        let c = resolve(&cli).unwrap();
        assert_eq!(c.redis_url, "redis://cli:1");
        assert_eq!(c.stream, "cli:events");
        assert_eq!(c.max_mb, 1000);
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn env_beats_file() {
        use std::os::unix::fs::PermissionsExt;
        let d = tmpdir("env");
        let p = d.join("wfdc.toml");
        std::fs::write(&p, "redis_url = \"redis://file:1\"\n").unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::env::set_var("WFDC_REDIS_URL", "redis://env:1");
        let c = resolve(&CliOverrides {
            config: Some(p),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(c.redis_url, "redis://env:1");
        std::env::remove_var("WFDC_REDIS_URL");
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn relative_data_dir_resolves_against_config_file_dir() {
        use std::os::unix::fs::PermissionsExt;
        let d = tmpdir("rel");
        let p = d.join("wfdc.toml");
        std::fs::write(&p, "data_dir = \"out\"\n").unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
        let c = resolve(&CliOverrides {
            config: Some(p),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(c.data_dir, d.join("out"));
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn max_mb_normalization() {
        assert_eq!(resolve(&CliOverrides::default()).unwrap().max_mb, 500);
        assert_eq!(
            resolve(&CliOverrides {
                max_mb: Some(0),
                ..Default::default()
            })
            .unwrap()
            .max_mb,
            500
        );
        assert_eq!(
            resolve(&CliOverrides {
                max_mb: Some(7),
                ..Default::default()
            })
            .unwrap()
            .max_mb,
            16
        );
        assert_eq!(
            resolve(&CliOverrides {
                max_mb: Some(100),
                ..Default::default()
            })
            .unwrap()
            .max_mb,
            100
        );
    }

    #[test]
    fn world_readable_config_is_fatal() {
        use std::os::unix::fs::PermissionsExt;
        let d = tmpdir("perm");
        let p = d.join("wfdc.toml");
        std::fs::write(&p, "redis_url = \"redis://x:1\"\n").unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).unwrap();
        let err = resolve(&CliOverrides {
            config: Some(p.clone()),
            ..Default::default()
        })
        .unwrap_err();
        assert!(matches!(err, Error::Fatal(_)));
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn missing_config_file_is_not_an_error() {
        std::env::remove_var("WFDC_REDIS_URL");
        let c = resolve(&CliOverrides {
            config: Some(PathBuf::from("/nonexistent/wfdc.toml")),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(c.redis_url, DEFAULT_REDIS_URL);
    }
}
