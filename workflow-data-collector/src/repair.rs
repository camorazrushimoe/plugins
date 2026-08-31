//! Startup repair (§3.2): a crash mid-append can leave a partial JSON line at
//! EOF. On startup, scan every JSONL file; if the last line does not end with
//! `\n`, truncate the partial line (drop it) and log the repair (file + bytes
//! dropped). Lab never sees a malformed line.

use std::path::{Path, PathBuf};

use crate::Error;

/// Walk `data_dir` for `*.jsonl` files and repair partial trailing lines.
/// Returns one log line per repaired file: "file + bytes dropped".
pub fn repair(data_dir: &Path) -> Result<Vec<String>, Error> {
    let mut logs = Vec::new();
    let mut files: Vec<PathBuf> = Vec::new();
    collect_jsonl(data_dir, &mut files);
    files.sort();

    for f in files {
        let bytes = std::fs::read(&f)?;
        if bytes.is_empty() || bytes[bytes.len() - 1] == b'\n' {
            continue;
        }
        // Find the last newline; drop everything after it.
        let keep = match bytes.iter().rposition(|&b| b == b'\n') {
            Some(pos) => pos + 1,
            None => 0, // whole file is one partial line
        };
        let dropped = bytes.len() - keep;
        let file = std::fs::OpenOptions::new().write(true).open(&f)?;
        file.set_len(keep as u64)?;
        logs.push(format!(
            "repaired {} (dropped {dropped} bytes)",
            f.display()
        ));
    }
    Ok(logs)
}

fn collect_jsonl(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_jsonl(&path, out);
        } else if path.extension().map(|e| e == "jsonl").unwrap_or(false) {
            out.push(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "wfdc-repair-{tag}-{}-{}",
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
    fn truncates_partial_trailing_line() {
        let d = tmpdir("partial");
        let f = d.join("raw").join("dt=2026-08-30").join("events.jsonl");
        std::fs::create_dir_all(f.parent().unwrap()).unwrap();
        std::fs::write(&f, "{\"a\":1}\n{\"a\":2}\n{\"a\":3").unwrap(); // partial last line

        let logs = repair(&d).unwrap();
        assert_eq!(logs.len(), 1);
        assert!(logs[0].contains("dropped 6 bytes"), "{:?}", logs[0]);

        let after = std::fs::read_to_string(&f).unwrap();
        assert_eq!(after, "{\"a\":1}\n{\"a\":2}\n");
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn whole_file_partial_line_truncates_to_empty() {
        let d = tmpdir("whole");
        let f = d.join("events.jsonl");
        std::fs::write(&f, "{\"a\":1}").unwrap();
        let logs = repair(&d).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(std::fs::read_to_string(&f).unwrap(), "");
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn clean_files_are_untouched() {
        let d = tmpdir("clean");
        let f = d.join("events.jsonl");
        std::fs::write(&f, "{\"a\":1}\n{\"a\":2}\n").unwrap();
        let logs = repair(&d).unwrap();
        assert!(logs.is_empty());
        assert_eq!(
            std::fs::read_to_string(&f).unwrap(),
            "{\"a\":1}\n{\"a\":2}\n"
        );
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn ignores_non_jsonl_files() {
        let d = tmpdir("other");
        std::fs::write(d.join("CHECKPOINT"), "1-0").unwrap();
        std::fs::write(d.join("MANIFEST.json"), "{\"x\":1}").unwrap(); // not jsonl
        let logs = repair(&d).unwrap();
        assert!(logs.is_empty());
        let _ = std::fs::remove_dir_all(&d);
    }
}
