#!/usr/bin/env bash
#
# build.sh — build the wfdc ship unit: one static musl x86_64 binary.
#
# Spec: workflow-data-collector/SPEC.md §9.8 (musl binary in bin/) and §2
# (install model: one static binary + config next to it). The README plugin
# contract ships the *install unit* as a prebuilt static musl binary; this
# script is the reproducible way to produce it from source.
#
# Reproducibility (engineering non-blocking finding):
#   - toolchain is PINNED below (rustup <version> + x86_64-unknown-linux-musl)
#     — no `latest` drift;
#   - the crate's [profile.release] sets strip=true, lto=true,
#     codegen-units=1, which removes build-path metadata;
#   - consequence: two clean builds of the same commit with the same pinned
#     toolchain produce byte-identical binaries (verified: identical sha256
#     across clean rebuilds and across different checkout paths).
#
# Outputs:
#   bin/wfdc      — the static musl binary
#   bin/SHA256SUMS — recorded sha256 of bin/wfdc (sha256sum -c compatible)
#
# Usage:
#   ./build.sh            # build, verify static, record sha256
#   ./build.sh --no-sha   # build + static check only (no SHA256SUMS rewrite)
#
# Requires: rustup, a pinned Rust toolchain (installs the musl target on
# first run), readelf or ldd (static check), sha256sum.
set -euo pipefail

# ---------------------------------------------------------------------------
# Toolchain pin (BUILD-2). Change deliberately; never to "latest".
# ---------------------------------------------------------------------------
RUST_TOOLCHAIN="1.98.0"
MUSL_TARGET="x86_64-unknown-linux-musl"

# ---------------------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"   # .../workflow-data-collector
CRATE_DIR="$HERE"
BIN_DIR="$HERE/bin"
BIN="$BIN_DIR/wfdc"
SUMS="$BIN_DIR/SHA256SUMS"
WRITE_SUMS=1
[ "${1:-}" = "--no-sha" ] && WRITE_SUMS=0

log()  { printf 'build.sh: %s\n' "$*"; }
die()  { printf 'build.sh: ERROR: %s\n' "$*" >&2; exit 1; }

[ -f "$CRATE_DIR/Cargo.toml" ] || die "no Cargo.toml in $CRATE_DIR — nothing to build"
grep -q '^name = "wfdc"' "$CRATE_DIR/Cargo.toml" || die "Cargo.toml is not the wfdc crate"

# --- 1. toolchain + target -------------------------------------------------
command -v rustup >/dev/null 2>&1 || die "rustup not found (RUSTUP_HOME=${RUSTUP_HOME:-<unset>})"
command -v cargo >/dev/null 2>&1 || die "cargo not found (CARGO_HOME=${CARGO_HOME:-<unset>})"

if ! rustup toolchain list | grep -q "^${RUST_TOOLCHAIN}"; then
    log "installing pinned toolchain ${RUST_TOOLCHAIN} (this is a one-time fetch)"
    rustup toolchain install "$RUST_TOOLCHAIN"
fi
if ! rustup target list --toolchain "$RUST_TOOLCHAIN" --installed | grep -q "^${MUSL_TARGET}$"; then
    log "adding musl target ${MUSL_TARGET} to toolchain ${RUST_TOOLCHAIN}"
    rustup target add "$MUSL_TARGET" --toolchain "$RUST_TOOLCHAIN"
fi

# --- 2. lockfile (reproducible dependency pins) -----------------------------
if [ ! -f "$CRATE_DIR/Cargo.lock" ]; then
    log "Cargo.lock missing — generating one; commit it for reproducibility"
    (cd "$CRATE_DIR" && rustup run "$RUST_TOOLCHAIN" cargo generate-lockfile)
fi

# --- 3. build ---------------------------------------------------------------
log "building wfdc with pinned toolchain ${RUST_TOOLCHAIN} (target ${MUSL_TARGET})"
(
    cd "$CRATE_DIR"
    rustup run "$RUST_TOOLCHAIN" cargo build --release --locked \
        --target "$MUSL_TARGET"
)

SRC_BIN="$CRATE_DIR/target/$MUSL_TARGET/release/wfdc"
[ -f "$SRC_BIN" ] || die "build produced no binary at $SRC_BIN"

# --- 4. static check (BUILD-3) ----------------------------------------------
check_static() {
    local b="$1" dynamic=0
    if command -v readelf >/dev/null 2>&1; then
        if readelf -l "$b" | grep -q 'INTERP'; then dynamic=1; fi
    fi
    if command -v ldd >/dev/null 2>&1; then
        if ! ldd "$b" 2>&1 | grep -qi 'statically linked'; then dynamic=1; fi
    fi
    [ "$dynamic" -eq 0 ] || die "binary $b is NOT statically linked"
}
check_static "$SRC_BIN"

# --- 5. install into bin/ + record sha256 -----------------------------------
mkdir -p "$BIN_DIR"
cp -f "$SRC_BIN" "$BIN"
chmod 0755 "$BIN"
check_static "$BIN"

SHA="$(sha256sum "$BIN" | awk '{print $1}')"
if [ "$WRITE_SUMS" -eq 1 ]; then
    (cd "$BIN_DIR" && sha256sum wfdc > SHA256SUMS)
    log "recorded sha256 in $SUMS"
fi

# --- 6. report ---------------------------------------------------------------
CRATE_VERSION="$(sed -n 's/^version = "\([^"]*\)".*/\1/p' "$CRATE_DIR/Cargo.toml" | head -1)"
log "built: $BIN"
log "  version  = ${CRATE_VERSION:-<unknown>} (Cargo.toml)"
log "  sha256   = $SHA"
if "$BIN" --version >/dev/null 2>&1; then
    log "  runtime  = $("$BIN" --version 2>&1 | head -1)"
else
    log "  runtime  = (binary does not implement --version yet)"
fi
log "done. Install unit: $BIN + wfdc.toml next to it (SPEC §2)."
