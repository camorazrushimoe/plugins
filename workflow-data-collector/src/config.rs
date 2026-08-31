//! Config load, precedence and permission enforcement (§2).
//!
//! Precedence: CLI > env > config file > built-in defaults. A **relative**
//! `data_dir` resolves against the directory of the config file that supplied
//! it; if no config file was used (env value or default), against the binary's
//! directory. A relative `WFDC_DATA_DIR` env value resolves against the
//! binary's directory. Never against the process cwd. The loaded config file
//! must not be group/world-readable (it holds the Redis password).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::cli::CliArgs;

pub const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1:6380";
pub const DEFAULT_STREAM: &str = "office:events";
pub const DEFAULT_DATA_DIR: &str = "./wfdc-data";
pub const DEFAULT_MAX_MB: u64 = 500;
pub const DEFAULT_EXPIRE_HOURS: u64 = 6;

/// Config file search order (§2): `--config`, `$WFDC_CONFIG`,
/// `<binary-dir>/wfdc.toml`, `./wfdc.toml`.
pub const CONFIG_FILENAME: &str = "wfdc.toml";

/// Resolved runtime configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub redis_url: String,
    pub stream: String,
    /// Absolute (relative values resolved per §2).
    pub data_dir: PathBuf,
    /// Hard cap on collected JSONL under data_dir (MB), normalized per §2:
    /// 0/negative/missing → 500, 1–15 → 16.
    pub max_mb: u64,
    /// Session expiry window in hours (§5.3), default 6.
    pub expire_hours: u64,
}

/// The inputs a [`Config`] is resolved from — injectable for tests.
pub struct Sources<'a> {
    pub cli: &'a CliArgs,
    pub env: &'a BTreeMap<String, String>,
    pub binary_dir: &'a Path,
    pub cwd: &'a Path,
}

/// Config error → fatal (exit 1, §3.4).
#[derive(Debug)]
pub struct ConfigError(pub String);

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ConfigError {}

impl Config {
    /// Resolve the configuration from CLI/env/file/defaults (§2).
    pub fn load(src: &Sources) -> Result<Config, ConfigError> {
        // --- locate the config file -------------------------------------------------
        let explicit = src
            .cli
            .config
            .clone()
            .or_else(|| non_empty(src.env, "WFDC_CONFIG").map(PathBuf::from));
        let config_file = match &explicit {
            Some(p) => {
                if !p.is_file() {
                    return Err(ConfigError(format!(
                        "config file {} does not exist",
                        p.display()
                    )));
                }
                Some(p.clone())
            }
            None => {
                let bin = src.binary_dir.join(CONFIG_FILENAME);
                let cwd = src.cwd.join(CONFIG_FILENAME);
                if bin.is_file() {
                    Some(bin)
                } else if cwd.is_file() {
                    Some(cwd)
                } else {
                    None
                }
            }
        };

        // --- permission enforcement: config file must be 0600 (no group/world) ----
        let file_cfg = match &config_file {
            Some(path) => {
                check_config_file_perms(path)?;
                let text = std::fs::read_to_string(path)
                    .map_err(|e| ConfigError(format!("cannot read {}: {e}", path.display())))?;
                Some(parse_file(&text, path)?)
            }
            None => None,
        };

        let file_base = config_file
            .as_deref()
            .and_then(|p| p.parent().map(Path::to_path_buf));

        // --- per-field precedence: CLI > env > file > default ----------------------
        let redis_url = first_non_empty(
            src.cli.redis.as_deref(),
            non_empty(src.env, "WFDC_REDIS_URL").as_deref(),
            file_cfg.as_ref().and_then(|f| f.redis_url.as_deref()),
            Some(DEFAULT_REDIS_URL),
        )
        .unwrap()
        .to_string();

        let stream = first_non_empty(
            src.cli.stream.as_deref(),
            non_empty(src.env, "WFDC_STREAM").as_deref(),
            file_cfg.as_ref().and_then(|f| f.stream.as_deref()),
            Some(DEFAULT_STREAM),
        )
        .unwrap()
        .to_string();

        // data_dir: env > file > default. CLI has no data-dir flag (§2).
        let (data_dir_value, from_file) = if let Some(v) = non_empty(src.env, "WFDC_DATA_DIR") {
            (v, false)
        } else if let Some(v) = file_cfg.as_ref().and_then(|f| f.data_dir.as_deref()) {
            (v.to_string(), true)
        } else {
            (DEFAULT_DATA_DIR.to_string(), false)
        };
        let data_dir = resolve_data_dir(
            &data_dir_value,
            from_file,
            file_base.as_deref(),
            src.binary_dir,
        );

        let max_mb = if let Some(v) = src.cli.max_mb {
            v
        } else if let Some(raw) = non_empty(src.env, "WFDC_MAX_MB") {
            parse_i64("WFDC_MAX_MB", &raw)?
        } else if let Some(v) = file_cfg.as_ref().and_then(|f| f.max_mb) {
            v
        } else {
            DEFAULT_MAX_MB as i64
        };
        let max_mb = normalize_max_mb(max_mb);

        let expire_hours = if let Some(v) = src.cli.expire_after {
            v
        } else if let Some(raw) = non_empty(src.env, "WFDC_EXPIRE_HOURS") {
            parse_i64("WFDC_EXPIRE_HOURS", &raw)?
        } else if let Some(v) = file_cfg.as_ref().and_then(|f| f.expire_hours) {
            v
        } else {
            DEFAULT_EXPIRE_HOURS as i64
        };
        let expire_hours = if expire_hours <= 0 {
            DEFAULT_EXPIRE_HOURS
        } else {
            expire_hours as u64
        };

        // --- validate the Redis URL up front: malformed → fatal config error ------
        if redis::Client::open(redis_url.as_str()).is_err() {
            return Err(ConfigError(format!("invalid redis_url {redis_url:?}")));
        }

        Ok(Config {
            redis_url,
            stream,
            data_dir,
            max_mb,
            expire_hours,
        })
    }
}

/// §2 normalization: 0/negative → 500, 1–15 → 16, else the value.
pub fn normalize_max_mb(v: i64) -> u64 {
    match v {
        ..=0 => DEFAULT_MAX_MB,
        1..=15 => 16,
        n => n as u64,
    }
}

fn resolve_data_dir(
    value: &str,
    from_file: bool,
    file_base: Option<&Path>,
    binary_dir: &Path,
) -> PathBuf {
    let p = PathBuf::from(value);
    if p.is_absolute() {
        return p;
    }
    let base = if from_file {
        file_base
            .map(Path::to_path_buf)
            .unwrap_or_else(|| binary_dir.to_path_buf())
    } else {
        binary_dir.to_path_buf()
    };
    base.join(p)
}

#[derive(serde::Deserialize, Default)]
struct FileConfig {
    redis_url: Option<String>,
    stream: Option<String>,
    data_dir: Option<String>,
    max_mb: Option<i64>,
    expire_hours: Option<i64>,
}

fn parse_file(text: &str, path: &Path) -> Result<FileConfig, ConfigError> {
    toml::from_str::<FileConfig>(text)
        .map_err(|e| ConfigError(format!("invalid config file {}: {e}", path.display())))
}

/// `wfdc.toml` holds the Redis password; warn and exit 1 if group/world-readable
/// (§2). "0600" is enforced as "no group/other permission bits".
fn check_config_file_perms(path: &Path) -> Result<(), ConfigError> {
    use std::os::unix::fs::PermissionsExt;
    let meta = std::fs::metadata(path)
        .map_err(|e| ConfigError(format!("cannot stat {}: {e}", path.display())))?;
    let mode = meta.permissions().mode();
    if mode & 0o077 != 0 {
        return Err(ConfigError(format!(
            "{} is group/world-readable (mode {:04o}) — wfdc.toml must be 0600 \
             (it holds the Redis password)",
            path.display(),
            mode & 0o777
        )));
    }
    Ok(())
}

fn non_empty(env: &BTreeMap<String, String>, key: &str) -> Option<String> {
    env.get(key).filter(|v| !v.is_empty()).cloned()
}

fn parse_i64(name: &str, raw: &str) -> Result<i64, ConfigError> {
    raw.parse::<i64>()
        .map_err(|_| ConfigError(format!("{name} expects an integer, got {raw:?}")))
}

fn first_non_empty<'a>(
    a: Option<&'a str>,
    b: Option<&'a str>,
    c: Option<&'a str>,
    d: Option<&'a str>,
) -> Option<&'a str> {
    a.or(b).or(c).or(d)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    struct Fixture {
        _tmp: tempfile::TempDir,
        bin: PathBuf,
        cwd: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let tmp = tempfile::tempdir().unwrap();
            let bin = tmp.path().join("bin");
            let cwd = tmp.path().join("cwd");
            std::fs::create_dir_all(&bin).unwrap();
            std::fs::create_dir_all(&cwd).unwrap();
            Fixture {
                _tmp: tmp,
                bin,
                cwd,
            }
        }

        fn write_config(&self, dir: &str, content: &str) -> PathBuf {
            let p = self._tmp.path().join(dir);
            std::fs::create_dir_all(&p).unwrap();
            let f = p.join(CONFIG_FILENAME);
            std::fs::write(&f, content).unwrap();
            set_mode(&f, 0o600);
            f
        }

        fn load(
            &self,
            cli: &CliArgs,
            env: &BTreeMap<String, String>,
        ) -> Result<Config, ConfigError> {
            Config::load(&Sources {
                cli,
                env,
                binary_dir: &self.bin,
                cwd: &self.cwd,
            })
        }
    }

    fn set_mode(p: &Path, mode: u32) {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(p, std::fs::Permissions::from_mode(mode)).unwrap();
    }

    #[test]
    fn all_defaults() {
        let fx = Fixture::new();
        let c = fx.load(&CliArgs::default(), &env(&[])).unwrap();
        assert_eq!(c.redis_url, DEFAULT_REDIS_URL);
        assert_eq!(c.stream, DEFAULT_STREAM);
        assert_eq!(c.data_dir, fx.bin.join(DEFAULT_DATA_DIR));
        assert_eq!(c.max_mb, 500);
        assert_eq!(c.expire_hours, 6);
    }

    #[test]
    fn file_values_used_and_relative_data_dir_resolves_vs_config_dir() {
        let fx = Fixture::new();
        let cfg = fx.write_config(
            "etc",
            "redis_url = \"redis://cfg:6379\"\nstream = \"cfg:events\"\ndata_dir = \"./data\"\n",
        );
        let cli = CliArgs {
            config: Some(cfg),
            ..Default::default()
        };
        let c = fx.load(&cli, &env(&[])).unwrap();
        assert_eq!(c.redis_url, "redis://cfg:6379");
        assert_eq!(c.stream, "cfg:events");
        assert_eq!(c.data_dir, fx._tmp.path().join("etc").join("data"));
    }

    #[test]
    fn env_overrides_file() {
        let fx = Fixture::new();
        let cfg = fx.write_config(
            "etc",
            "redis_url = \"redis://cfg:6379\"\nstream = \"cfg:events\"\n",
        );
        let e = env(&[("WFDC_STREAM", "env:events"), ("WFDC_MAX_MB", "2000")]);
        let cli = CliArgs {
            config: Some(cfg),
            ..Default::default()
        };
        let c = fx.load(&cli, &e).unwrap();
        assert_eq!(c.stream, "env:events");
        assert_eq!(
            c.redis_url, "redis://cfg:6379",
            "file still fills unset fields"
        );
        assert_eq!(c.max_mb, 2000);
    }

    #[test]
    fn cli_overrides_env_and_file() {
        let fx = Fixture::new();
        let cfg = fx.write_config(
            "etc",
            "redis_url = \"redis://cfg:6379\"\nstream = \"cfg:events\"\n",
        );
        let cli = CliArgs {
            config: Some(cfg),
            redis: Some("redis://cli:1".into()),
            stream: Some("cli:events".into()),
            max_mb: Some(42),
            ..Default::default()
        };
        let c = fx
            .load(&cli, &env(&[("WFDC_STREAM", "env:events")]))
            .unwrap();
        assert_eq!(c.redis_url, "redis://cli:1");
        assert_eq!(c.stream, "cli:events");
        assert_eq!(c.max_mb, 42);
    }

    #[test]
    fn relative_wfdc_data_dir_resolves_vs_binary_dir() {
        let fx = Fixture::new();
        let c = fx
            .load(&CliArgs::default(), &env(&[("WFDC_DATA_DIR", "env-data")]))
            .unwrap();
        assert_eq!(c.data_dir, fx.bin.join("env-data"));
    }

    #[test]
    fn absolute_data_dir_kept() {
        let fx = Fixture::new();
        let abs = fx._tmp.path().join("abs-data");
        let c = fx
            .load(
                &CliArgs::default(),
                &env(&[("WFDC_DATA_DIR", abs.to_str().unwrap())]),
            )
            .unwrap();
        assert_eq!(c.data_dir, abs);
    }

    #[test]
    fn file_absolute_data_dir_kept() {
        let fx = Fixture::new();
        let abs = fx._tmp.path().join("abs-data");
        let cfg = fx.write_config("etc", &format!("data_dir = \"{}\"\n", abs.display()));
        let cli = CliArgs {
            config: Some(cfg),
            ..Default::default()
        };
        let c = fx.load(&cli, &env(&[])).unwrap();
        assert_eq!(c.data_dir, abs);
    }

    #[test]
    fn max_mb_normalization() {
        let fx = Fixture::new();
        for (raw, expect) in [
            (0i64, 500u64),
            (-5, 500),
            (1, 16),
            (15, 16),
            (16, 16),
            (1000, 1000),
        ] {
            let cli = CliArgs {
                max_mb: Some(raw),
                ..Default::default()
            };
            let c = fx.load(&cli, &env(&[])).unwrap();
            assert_eq!(c.max_mb, expect, "max_mb {raw}");
        }
    }

    #[test]
    fn max_mb_bad_env_value_is_error() {
        let fx = Fixture::new();
        assert!(fx
            .load(&CliArgs::default(), &env(&[("WFDC_MAX_MB", "abc")]))
            .is_err());
    }

    #[test]
    fn config_file_must_not_be_group_or_world_readable() {
        let fx = Fixture::new();
        let f = fx.write_config("etc", "redis_url = \"redis://x:1\"\n");
        set_mode(&f, 0o644);
        let cli = CliArgs {
            config: Some(f.clone()),
            ..Default::default()
        };
        let err = fx.load(&cli, &env(&[])).unwrap_err();
        assert!(err.0.contains("0600"), "got: {err}");
        // 0400 (owner-only, tighter) is fine
        set_mode(&f, 0o400);
        assert!(fx.load(&cli, &env(&[])).is_ok());
    }

    #[test]
    fn explicit_config_missing_is_error() {
        let fx = Fixture::new();
        let cli = CliArgs {
            config: Some(fx._tmp.path().join("nope.toml")),
            ..Default::default()
        };
        assert!(fx.load(&cli, &env(&[])).is_err());
        let e = env(&[(
            "WFDC_CONFIG",
            fx._tmp.path().join("nope.toml").to_str().unwrap(),
        )]);
        assert!(fx.load(&CliArgs::default(), &e).is_err());
    }

    #[test]
    fn search_order_binary_dir_before_cwd() {
        let fx = Fixture::new();
        fx.write_config("bin", "stream = \"from-bin\"\n");
        fx.write_config("cwd", "stream = \"from-cwd\"\n");
        let c = fx.load(&CliArgs::default(), &env(&[])).unwrap();
        assert_eq!(c.stream, "from-bin");
    }

    #[test]
    fn falls_back_to_cwd_config() {
        let fx = Fixture::new();
        fx.write_config("cwd", "stream = \"from-cwd\"\n");
        let c = fx.load(&CliArgs::default(), &env(&[])).unwrap();
        assert_eq!(c.stream, "from-cwd");
    }

    #[test]
    fn empty_env_values_treated_as_unset() {
        let fx = Fixture::new();
        let c = fx
            .load(
                &CliArgs::default(),
                &env(&[("WFDC_STREAM", ""), ("WFDC_REDIS_URL", "")]),
            )
            .unwrap();
        assert_eq!(c.stream, DEFAULT_STREAM);
        assert_eq!(c.redis_url, DEFAULT_REDIS_URL);
    }

    #[test]
    fn malformed_redis_url_is_config_error() {
        let fx = Fixture::new();
        let cli = CliArgs {
            redis: Some("not a url".into()),
            ..Default::default()
        };
        assert!(fx.load(&cli, &env(&[])).is_err());
    }

    #[test]
    fn invalid_toml_is_error() {
        let fx = Fixture::new();
        let cfg = fx.write_config("etc", "this is not toml [[[\n");
        let cli = CliArgs {
            config: Some(cfg),
            ..Default::default()
        };
        assert!(fx.load(&cli, &env(&[])).is_err());
    }
}
