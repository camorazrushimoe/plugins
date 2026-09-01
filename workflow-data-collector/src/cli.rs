//! CLI argument parsing (§2).
//!
//! Hand-rolled on purpose: precedence (CLI > env > file > defaults) requires
//! distinguishing "flag not passed" from "flag passed", and the surface is
//! small. `--config`, `--redis`, `--stream`, `--max-mb`, `--expire-after`,
//! `-h`/`--help`.

use std::path::PathBuf;

/// Parsed command line. `None` means "not passed" — env/file fill the gap.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CliArgs {
    pub config: Option<PathBuf>,
    pub redis: Option<String>,
    pub stream: Option<String>,
    pub max_mb: Option<i64>,
    pub expire_after: Option<i64>,
    pub help: bool,
}

/// Parse argv (including the program name at index 0, which is skipped).
pub fn parse<I: Iterator<Item = String>>(args: I) -> Result<CliArgs, String> {
    let mut out = CliArgs::default();
    let mut it = args.skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            // `follow` is the default command; the explicit alias is accepted
            // here so `wfdc follow` works (§3.4 usage block). Other
            // subcommands (backfill, status) land with their own tickets.
            "follow" => {}
            "-h" | "--help" => out.help = true,
            "--config" | "--redis" | "--stream" | "--max-mb" | "--expire-after" => {
                let value = it.next().ok_or_else(|| format!("{arg} requires a value"))?;
                match arg.as_str() {
                    "--config" => out.config = Some(PathBuf::from(value)),
                    "--redis" => out.redis = Some(value),
                    "--stream" => out.stream = Some(value),
                    "--max-mb" => {
                        let n: i64 = value
                            .parse()
                            .map_err(|_| format!("--max-mb expects an integer, got {value:?}"))?;
                        out.max_mb = Some(n);
                    }
                    "--expire-after" => {
                        let n: i64 = value.parse().map_err(|_| {
                            format!("--expire-after expects an integer, got {value:?}")
                        })?;
                        out.expire_after = Some(n);
                    }
                    _ => unreachable!(),
                }
            }
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    Ok(out)
}

pub const USAGE: &str = "\
wfdc — Workflow Data Collector (spec v0.3.0)

USAGE:
    wfdc [OPTIONS]

OPTIONS:
    --config <PATH>       Config file (else $WFDC_CONFIG, <binary-dir>/wfdc.toml, ./wfdc.toml)
    --redis <URL>         Redis URL (else $WFDC_REDIS_URL, config file, redis://127.0.0.1:6380)
    --stream <NAME>       Stream name (else $WFDC_STREAM, config file, office:events)
    --max-mb <N>          JSONL cap in MB (0→500, 1–15→16; else $WFDC_MAX_MB, file, 500)
    --expire-after <H>    Session expiry window in hours (default 6; test knob)
    -h, --help            Print this help

The default command is follow: blocking XREAD on the stream, writing the raw
dataset to data_dir. SIGTERM/SIGINT flush and exit 0; a second signal exits 1.
";

#[cfg(test)]
mod tests {
    use super::*;

    fn argv<'a>(items: &'a [&'a str]) -> impl Iterator<Item = String> + 'a {
        items.iter().map(|s| s.to_string())
    }

    #[test]
    fn no_args_is_default() {
        let c = parse(argv(&["wfdc"])).unwrap();
        assert_eq!(c, CliArgs::default());
    }

    #[test]
    fn parses_all_flags() {
        let c = parse(argv(&[
            "wfdc",
            "--config",
            "/etc/wfdc.toml",
            "--redis",
            "redis://x:1",
            "--stream",
            "s",
            "--max-mb",
            "42",
            "--expire-after",
            "2",
        ]))
        .unwrap();
        assert_eq!(
            c.config.as_deref(),
            Some(std::path::Path::new("/etc/wfdc.toml"))
        );
        assert_eq!(c.redis.as_deref(), Some("redis://x:1"));
        assert_eq!(c.stream.as_deref(), Some("s"));
        assert_eq!(c.max_mb, Some(42));
        assert_eq!(c.expire_after, Some(2));
    }

    #[test]
    fn help_flag() {
        assert!(parse(argv(&["wfdc", "--help"])).unwrap().help);
        assert!(parse(argv(&["wfdc", "-h"])).unwrap().help);
    }

    #[test]
    fn explicit_follow_subcommand_is_accepted() {
        assert_eq!(
            parse(argv(&["wfdc", "follow"])).unwrap(),
            CliArgs::default()
        );
        assert_eq!(
            parse(argv(&["wfdc", "follow", "--redis", "redis://x:1"])).unwrap(),
            CliArgs {
                redis: Some("redis://x:1".into()),
                ..Default::default()
            }
        );
    }

    #[test]
    fn unknown_subcommand_is_error() {
        // backfill/status arrive with their own tickets (BON-72/BON-70)
        assert!(parse(argv(&["wfdc", "backfill"])).is_err());
    }

    #[test]
    fn missing_value_is_error() {
        assert!(parse(argv(&["wfdc", "--redis"])).is_err());
    }

    #[test]
    fn unknown_flag_is_error() {
        assert!(parse(argv(&["wfdc", "--nope"])).is_err());
    }

    #[test]
    fn bad_number_is_error() {
        assert!(parse(argv(&["wfdc", "--max-mb", "abc"])).is_err());
        assert!(parse(argv(&["wfdc", "--expire-after", "x"])).is_err());
    }

    #[test]
    fn negative_max_mb_accepted_for_normalization() {
        let c = parse(argv(&["wfdc", "--max-mb", "-5"])).unwrap();
        assert_eq!(c.max_mb, Some(-5));
    }
}
