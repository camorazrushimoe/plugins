#!/usr/bin/env bash
#
# verify-ship-unit.sh — verify the wfdc ship unit (BON-73 acceptance checks).
#
# Runs the QA matrix from ticket BON-73 against the committed ship unit:
#   BUILD-1  bin/wfdc exists (committed binary)
#   BUILD-2  build script pins the toolchain (no `latest` drift)
#   BUILD-3  binary is statically linked (readelf: no INTERP; ldd: static)
#   BUILD-4  reproducible: two clean builds of the same commit with the same
#            pinned toolchain produce identical sha256 (slow — needs --repro)
#   BIN-1    recorded sha256 in bin/SHA256SUMS matches the committed binary
#   BIN-2    binary version matches the crate version (the version MANIFEST.json
#            reports — SPEC §5.4 plugin_version)
#
# Usage:
#   ./verify-ship-unit.sh            # BUILD-1..3, BIN-1, BIN-2
#   ./verify-ship-unit.sh --repro    # + BUILD-4 (two clean rebuilds, slow)
#
# Exit 0 when every check passes; 1 otherwise. Prints a PASS/FAIL report.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CRATE_DIR="$HERE"
BIN_DIR="$HERE/bin"
BIN="$BIN_DIR/wfdc"
SUMS="$BIN_DIR/SHA256SUMS"
BUILD_SCRIPT="$HERE/build.sh"
DO_REPRO=0
[ "${1:-}" = "--repro" ] && DO_REPRO=1

RUST_TOOLCHAIN="$(sed -n 's/^RUST_TOOLCHAIN="\(.*\)"/\1/p' "$BUILD_SCRIPT" | head -1)"
MUSL_TARGET="x86_64-unknown-linux-musl"
CRATE_VERSION="$(sed -n 's/^version = "\([^"]*\)".*/\1/p' "$CRATE_DIR/Cargo.toml" | head -1)"

pass=0; fail=0
ok()   { printf '  PASS  %s\n' "$1"; pass=$((pass+1)); }
bad()  { printf '  FAIL  %s\n' "$1"; fail=$((fail+1)); }

echo "== wfdc ship-unit verification =="
echo "   crate version : ${CRATE_VERSION:-<missing>}"
echo "   pinned toolchain: ${RUST_TOOLCHAIN:-<missing>} (target $MUSL_TARGET)"

# --- BUILD-1 ---------------------------------------------------------------
echo "[BUILD-1] committed binary present"
if [ -f "$BIN" ] && [ -x "$BIN" ]; then ok "bin/wfdc exists and is executable"; else bad "bin/wfdc missing"; fi

# --- BUILD-2 ---------------------------------------------------------------
echo "[BUILD-2] toolchain pinned in build.sh"
if [ -n "$RUST_TOOLCHAIN" ] && [ "$RUST_TOOLCHAIN" != "latest" ]; then
    ok "build.sh pins RUST_TOOLCHAIN=$RUST_TOOLCHAIN"
else
    bad "build.sh does not pin a concrete toolchain"
fi
if grep -q 'MUSL_TARGET="x86_64-unknown-linux-musl"' "$BUILD_SCRIPT"; then
    ok "build.sh pins musl target $MUSL_TARGET"
else
    bad "build.sh does not pin the musl target"
fi

# --- BUILD-3 ---------------------------------------------------------------
echo "[BUILD-3] statically linked"
if [ -f "$BIN" ]; then
    static=1
    if command -v readelf >/dev/null 2>&1 && readelf -l "$BIN" 2>/dev/null | grep -q 'INTERP'; then
        static=0
    fi
    if command -v ldd >/dev/null 2>&1 && ! ldd "$BIN" 2>&1 | grep -qi 'statically linked'; then
        static=0
    fi
    if [ "$static" -eq 1 ]; then ok "no dynamic interpreter; ldd reports statically linked"; else bad "binary has a dynamic dependency"; fi
else
    bad "cannot check static link (no binary)"
fi

# --- BIN-1 -----------------------------------------------------------------
echo "[BIN-1] recorded sha256 matches the committed binary"
if [ -f "$BIN" ] && [ -f "$SUMS" ]; then
    if (cd "$BIN_DIR" && sha256sum -c SHA256SUMS >/dev/null 2>&1); then
        ok "sha256sum -c SHA256SUMS verified ($(awk '{print $1}' "$SUMS"))"
    else
        bad "sha256 mismatch vs SHA256SUMS"
    fi
else
    bad "binary or SHA256SUMS missing"
fi

# --- BIN-2 -----------------------------------------------------------------
echo "[BIN-2] binary version == crate version (== MANIFEST plugin_version)"
if [ -f "$BIN" ] && [ -n "$CRATE_VERSION" ]; then
    if "$BIN" --version >/dev/null 2>&1; then
        BIN_VERSION="$("$BIN" --version 2>&1 | head -1)"
        if echo "$BIN_VERSION" | grep -q "$CRATE_VERSION"; then
            ok "binary reports $BIN_VERSION (matches crate $CRATE_VERSION)"
        else
            bad "binary reports '$BIN_VERSION', crate is $CRATE_VERSION"
        fi
    else
        bad "binary does not implement --version (MANIFEST plugin_version source)"
    fi
else
    bad "cannot check version (binary or crate version missing)"
fi

# --- BUILD-4 (optional) ----------------------------------------------------
if [ "$DO_REPRO" -eq 1 ]; then
    echo "[BUILD-4] reproducibility: two clean builds → identical sha256"
    if [ -f "$CRATE_DIR/Cargo.toml" ]; then
        TMP1="$(mktemp -d)"; TMP2="$(mktemp -d)"
        (cd "$CRATE_DIR" && rustup run "$RUST_TOOLCHAIN" cargo build --release --locked --target "$MUSL_TARGET" 2>/dev/null \
            && sha256sum "target/$MUSL_TARGET/release/wfdc" | awk '{print $1}' > "$TMP1/hash")
        (cd "$CRATE_DIR" && rm -rf target \
            && rustup run "$RUST_TOOLCHAIN" cargo build --release --locked --target "$MUSL_TARGET" 2>/dev/null \
            && sha256sum "target/$MUSL_TARGET/release/wfdc" | awk '{print $1}' > "$TMP2/hash")
        H1="$(cat "$TMP1/hash" 2>/dev/null || true)"; H2="$(cat "$TMP2/hash" 2>/dev/null || true)"
        rm -rf "$TMP1" "$TMP2"
        if [ -n "$H1" ] && [ "$H1" = "$H2" ]; then
            ok "clean builds identical: $H1"
        else
            bad "reproducibility delta: '$H1' vs '$H2'"
        fi
    else
        bad "no crate to rebuild"
    fi
else
    echo "[BUILD-4] skipped (pass --repro for the two-clean-build check)"
fi

echo
echo "result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
