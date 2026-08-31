//! CLI argument parsing (§2).
//!
//! Hand-rolled on purpose: precedence (CLI > env > file > defaults) requires
//! distinguishing "flag not passed" from "flag passed", and the surface is
//! small. `--config`, `--redis`, `--stream`, `--max-mb`, `--expire-after`,
//! `-h`/`--help`.

use std::path::PathBuf;

/// Parsed command line. `None` means "not passed" — env/file fill the gap.
#[derive(Debug, Clone, PartialEq)]
pub struct CliArgs {
    pub config: Option<PathBuf>,
    pub redis: Option<String>,
    pub stream: Option<String>,
    pub max_mb: Option<i64>,
    pub expire_after: Option<i64>,
    /// `--once`: one XREAD batch then clean stop (§3.4) — equivalent to
    /// `--max-reads 1`. Mutually exclusive with `--max-reads`.
    pub once: bool,
    /// `--max-reads N`: N XREAD batches then clean stop (§3.4). `N >= 1`.
    pub max_reads: Option<i64>,
    /// `--max-idle-ms MS`: clean stop when no event arrives for MS (§3.4).
    /// `MS >= 0` (0 = immediate stop after the first read iteration).
    pub max_idle_ms: Option<i64>,
    pub help: bool,
    pub command: Command,
}

impl Default for CliArgs {
    fn default() -> Self {
        CliArgs {
            config: None,
            redis: None,
            stream: None,
            max_mb: None,
            expire_after: None,
            help: false,
            command: Command::Follow,
        }
    }
}

/// The invoked subcommand. `follow` is the default; `backfill` replays a
/// chosen stream range (§3.5, §9 item 7).
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Follow,
    Backfill {
        /// First stream id of the range, inclusive (default `0` = stream start).
        from: String,
        /// Last stream id of the range, inclusive (default `+` = stream end).
        to: String,
    },
}

/// Parse argv (including the program name at index 0, which is skipped).
pub fn parse<I: Iterator<Item = String>>(args: I) -> Result<CliArgs, String> {
    let mut out = CliArgs::default();
    let mut it = args.skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            // `follow` is the default command; the explicit alias is accepted
            // here so `wfdc follow` works (§3.4 usage block).
            "follow" => out.command = Command::Follow,
            // `backfill` consumes the rest of the line: --from/--to (defaults
            // 0 and +). Global flags must precede the subcommand (§3.5).
            "backfill" => {
                let mut from = "0".to_string();
                let mut to = "+".to_string();
                while let Some(flag) = it.next() {
                    match flag.as_str() {
                        "--from" => {
                            from = it
                                .next()
                                .ok_or_else(|| "--from requires a value".to_string())?
                        }
                        "--to" => {
                            to = it
                                .next()
                                .ok_or_else(|| "--to requires a value".to_string())?
                        }
                        other => return Err(format!("unknown argument {other:?}")),
                    }
                }
                out.command = Command::Backfill { from, to };
            }
            "-h" | "--help" => out.help = true,
            "--once" => out.once = true,
            "--config" | "--redis" | "--stream" | "--max-mb" | "--expire-after" | "--max-reads"
            | "--max-idle-ms" => {
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
                    "--max-reads" => {
                        let n: i64 = value.parse().map_err(|_| {
                            format!("--max-reads expects an integer, got {value:?}")
                        })?;
                        if n < 1 {
                            return Err(format!("--max-reads expects N >= 1, got {n}"));
                        }
                        out.max_reads = Some(n);
                    }
                    "--max-idle-ms" => {
                        let n: i64 = value.parse().map_err(|_| {
                            format!("--max-idle-ms expects an integer, got {value:?}")
                        })?;
                        if n < 0 {
                            return Err(format!("--max-idle-ms expects MS >= 0, got {n}"));
                        }
                        out.max_idle_ms = Some(n);
                    }
                    _ => unreachable!(),
                }
            }
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    if out.once && out.max_reads.is_some() {
        return Err("--once cannot be combined with --max-reads (--once ≡ --max-reads 1)".into());
    }
    Ok(out)
}

impl CliArgs {
    /// §3.4 `--once` ≡ `--max-reads 1`: the effective batch count for the
    /// follow loop, or `None` when neither flag is passed (follow forever).
    pub fn follow_max_reads(&self) -> Option<usize> {
        if self.once {
            Some(1)
        } else {
            self.max_reads.map(|n| n as usize)
        }
    }
}

pub const USAGE: &str = "\
wfdc — Workflow Data Collector (spec v0.3.0)

USAGE:
    wfdc [OPTIONS]
    wfdc [OPTIONS] follow
    wfdc [OPTIONS] backfill [--from STREAM_ID] [--to STREAM_ID]

OPTIONS:
    --config <PATH>       Config file (else $WFDC_CONFIG, <binary-dir>/wfdc.toml, ./wfdc.toml)
    --redis <URL>         Redis URL (else $WFDC_REDIS_URL, config file, redis://127.0.0.1:6380)
    --stream <NAME>       Stream name (else $WFDC_STREAM, config file, office:events)
    --max-mb <N>          JSONL cap in MB (0→500, 1–15→16; else $WFDC_MAX_MB, file, 500)
    --expire-after <H>    Session expiry window in hours (default 6; test knob)
    --once                Read one XREAD batch, flush, CHECKPOINT, exit 0 (§3.4; ≡ --max-reads 1)
    --max-reads <N>       N XREAD batches (empty batches count), then clean stop (§3.4); N >= 1
    --max-idle-ms <MS>    Clean stop when no event arrives for MS (§3.4); MS >= 0 (0 = immediate)
    -h, --help            Print this help

The default command is follow: blocking XREAD on the stream, writing the raw
dataset to data_dir. SIGTERM/SIGINT flush and exit 0; a second signal exits 1.

backfill replays a chosen inclusive range [--from, --to] (defaults 0 and +)
with the same writer, decoder and pairing rules as follow (§3.5): dedupe
applies (an entry at/below the resume point — max of the durable CHECKPOINT
and the highest id already written to JSONL — is skipped), CHECKPOINT moves
forward only, and an inverted/empty range writes nothing and exits 0.

All stop triggers share one clean-stop path: flush → CHECKPOINT → exit 0
(the max_mb cap step lands with the disk-cap feature, §5.5).
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
        // status arrives with its own ticket (BON-70)
        assert!(parse(argv(&["wfdc", "status"])).is_err());
    }

    #[test]
    fn backfill_defaults_to_full_range() {
        let c = parse(argv(&["wfdc", "backfill"])).unwrap();
        assert_eq!(
            c.command,
            Command::Backfill {
                from: "0".into(),
                to: "+".into()
            }
        );
    }

    #[test]
    fn backfill_parses_from_and_to() {
        let c = parse(argv(&[
            "wfdc",
            "backfill",
            "--from",
            "1725062400000-0",
            "--to",
            "1725062400099-0",
        ]))
        .unwrap();
        assert_eq!(
            c.command,
            Command::Backfill {
                from: "1725062400000-0".into(),
                to: "1725062400099-0".into()
            }
        );
    }

    #[test]
    fn backfill_accepts_only_from() {
        let c = parse(argv(&["wfdc", "backfill", "--from", "5-0"])).unwrap();
        assert_eq!(
            c.command,
            Command::Backfill {
                from: "5-0".into(),
                to: "+".into()
            }
        );
    }

    #[test]
    fn backfill_missing_value_is_error() {
        assert!(parse(argv(&["wfdc", "backfill", "--from"])).is_err());
        assert!(parse(argv(&["wfdc", "backfill", "--to"])).is_err());
    }

    #[test]
    fn backfill_unknown_flag_is_error() {
        assert!(parse(argv(&["wfdc", "backfill", "--nope"])).is_err());
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

    #[test]
    fn once_flag_parses() {
        let c = parse(argv(&["wfdc", "--once"])).unwrap();
        assert!(c.once);
        assert_eq!(c.max_reads, None);
    }

    #[test]
    fn max_reads_parses() {
        let c = parse(argv(&["wfdc", "--max-reads", "10"])).unwrap();
        assert!(!c.once);
        assert_eq!(c.max_reads, Some(10));
    }

    #[test]
    fn max_idle_ms_parses() {
        let c = parse(argv(&["wfdc", "--max-idle-ms", "5000"])).unwrap();
        assert_eq!(c.max_idle_ms, Some(5000));
    }

    #[test]
    fn all_stop_flags_parse_together() {
        let c = parse(argv(&["wfdc", "--max-reads", "3", "--max-idle-ms", "1500"])).unwrap();
        assert_eq!(c.max_reads, Some(3));
        assert_eq!(c.max_idle_ms, Some(1500));
    }

    #[test]
    fn once_combined_with_max_reads_is_error() {
        assert!(parse(argv(&["wfdc", "--once", "--max-reads", "2"])).is_err());
    }

    #[test]
    fn max_reads_zero_or_negative_is_error() {
        assert!(parse(argv(&["wfdc", "--max-reads", "0"])).is_err());
        assert!(parse(argv(&["wfdc", "--max-reads", "-1"])).is_err());
        assert!(parse(argv(&["wfdc", "--max-reads", "abc"])).is_err());
    }

    #[test]
    fn max_idle_negative_is_error_zero_is_ok() {
        assert!(parse(argv(&["wfdc", "--max-idle-ms", "-1"])).is_err());
        assert!(parse(argv(&["wfdc", "--max-idle-ms", "abc"])).is_err());
        // 0 is allowed: "0 idle tolerance" → immediate clean stop (IDL-4 pin)
        assert_eq!(
            parse(argv(&["wfdc", "--max-idle-ms", "0"]))
                .unwrap()
                .max_idle_ms,
            Some(0)
        );
    }

    #[test]
    fn follow_max_reads_none_without_flags() {
        assert_eq!(CliArgs::default().follow_max_reads(), None);
        assert_eq!(
            parse(argv(&["wfdc", "--max-idle-ms", "500"]))
                .unwrap()
                .follow_max_reads(),
            None
        );
    }

    #[test]
    fn follow_max_reads_once_equals_one() {
        assert_eq!(
            parse(argv(&["wfdc", "--once"]))
                .unwrap()
                .follow_max_reads(),
            Some(1)
        );
        assert_eq!(
            parse(argv(&["wfdc", "--max-reads", "10"]))
                .unwrap()
                .follow_max_reads(),
            Some(10)
        );
    }
}
