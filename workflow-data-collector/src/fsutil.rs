//! Small filesystem helpers shared by the store modules.

use std::path::Path;

use crate::Error;

/// Open (create) a file with mode 0600 regardless of umask. With
/// `append=true` the file is appended to; otherwise it is truncated.
pub fn open_private(path: &Path, append: bool) -> Result<std::fs::File, Error> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).mode(0o600);
    if append {
        opts.append(true);
    } else {
        opts.truncate(true);
    }
    let f = opts
        .open(path)
        .map_err(|e| Error::Io(format!("create {}: {e}", path.display())))?;
    f.set_permissions(std::fs::Permissions::from_mode(0o600))
        .map_err(|e| Error::Io(format!("chmod {}: {e}", path.display())))?;
    Ok(f)
}
