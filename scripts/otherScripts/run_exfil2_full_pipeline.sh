#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# run_exfil2_full_pipeline.sh
# Canonical runner for Mycelium multi-attack phases + path normalization
#
# Canonical phases dir:
#   ~/work/mycelium_ids/runs/<TS>/phases_safe_multi
# Compatibility symlink:
#   ~/work/mycelium_ids/lab/flows/phases_safe_multi -> canonical
#
# Usage:
#   bash scripts/run_exfil2_full_pipeline.sh
#
# Optional env knobs:
#   N=100 IFACE_USE=vethB DUR_CAPTURE=30 MODE=fg|bg
#   CAPTURE_SCRIPT=... (default: scripts/lab_capture_four_phase_safe.sh)
#   PHASES_NAME=phases_safe_multi  (or phases_safe)
#   REQUIRE_MULTI=1  (fail if benign+burst+lowslow not present)
#   DO_BUNDLE=1      (zip artifacts to bundles/)
# ============================================================

ROOT="${ROOT:-$HOME/work/mycelium_ids}"
SCRIPTS="$ROOT/scripts"
LAB_FLOWS="$ROOT/lab/flows"
RUNS="$ROOT/runs"
BUNDLES="$ROOT/bundles"

TS="${TS:-$(date +%Y%m%d_%H%M%S)}"
RUN="${RUN:-$RUNS/$TS}"

N="${N:-100}"
IFACE_USE="${IFACE_USE:-vethB}"
DUR_CAPTURE="${DUR_CAPTURE:-30}"
MODE="${MODE:-fg}" # fg | bg

CAPTURE_SCRIPT="${CAPTURE_SCRIPT:-$SCRIPTS/lab_capture_four_phase_safe.sh}"
PHASES_NAME="${PHASES_NAME:-phases_safe_multi}"
REQUIRE_MULTI="${REQUIRE_MULTI:-1}"
DO_BUNDLE="${DO_BUNDLE:-1}"

CANON_PHASES="$RUN/$PHASES_NAME"
COMPAT_PHASES="$LAB_FLOWS/$PHASES_NAME"

LOG_MAIN="$RUN/lab_capture_four_phase.log"

mkdir -p "$RUN" "$CANON_PHASES" "$LAB_FLOWS" "$BUNDLES"

echo "[INFO] ROOT=$ROOT"
echo "[INFO] RUN=$RUN"
echo "[INFO] CANON_PHASES=$CANON_PHASES"
echo "[INFO] COMPAT_PHASES=$COMPAT_PHASES"
echo "[INFO] CAPTURE_SCRIPT=$CAPTURE_SCRIPT"
echo "[INFO] N=$N IFACE_USE=$IFACE_USE DUR_CAPTURE=$DUR_CAPTURE MODE=$MODE"
echo

# -------- helpers --------
die() { echo "[ERROR] $*" >&2; exit 1; }

csv_count() {
  local dir="$1"
  shopt -s nullglob
  local files=("$dir"/*.csv)
  shopt -u nullglob
  echo "${#files[@]}"
}

move_into_canon_if_needed() {
  local src="$1"
  local dst="$2"
  if [[ -d "$src" && "$src" != "$dst" ]]; then
    local c
    c="$(csv_count "$src")"
    if [[ "$c" -gt 0 ]]; then
      echo "[INFO] Found CSVs in $src ($c). Moving into canonical: $dst"
      mkdir -p "$dst"
      # move only CSVs; keep non-csv in place
      shopt -s nullglob
      mv -f "$src"/*.csv "$dst"/
      shopt -u nullglob
    fi
  fi
}

ensure_symlink_compat() {
  local target="$1"
  local link="$2"
  mkdir -p "$(dirname "$link")"
  ln -sfn "$target" "$link"
  echo "[INFO] Symlinked: $link -> $target"
}

assert_expected_multi() {
  local dir="$1"
  local ok=0
  shopt -s nullglob
  local benign=("$dir"/benign_*.csv)
  local burst=("$dir"/attack_exfil_burst_*.csv)
  local lowslow=("$dir"/attack_exfil_lowslow_*.csv)
  shopt -u nullglob

  [[ "${#benign[@]}" -gt 0 ]] || ok=1
  [[ "${#burst[@]}" -gt 0 ]] || ok=1
  [[ "${#lowslow[@]}" -gt 0 ]] || ok=1

  if [[ "$ok" -ne 0 ]]; then
    echo "[DEBUG] benign csv:   ${#benign[@]}"
    echo "[DEBUG] burst csv:    ${#burst[@]}"
    echo "[DEBUG] lowslow csv:  ${#lowslow[@]}"
    die "Multi-attack phase CSVs missing in $dir (expected benign + attack_exfil_burst + attack_exfil_lowslow)."
  fi
}

# -------- capture run --------
run_capture_fg() {
  echo "[INFO] Running capture in foreground..."
  # Not assuming CAPTURE_SCRIPT supports all vars; we still pass common ones.
  # If ignored, normalization below will still fix output placement.
  bash "$CAPTURE_SCRIPT" \
    RUN="$RUN" TS="$TS" N="$N" IFACE_USE="$IFACE_USE" DUR_CAPTURE="$DUR_CAPTURE" \
    |& tee "$LOG_MAIN"
}

run_capture_bg() {
  echo "[INFO] Running capture in background (nohup)..."
  nohup bash "$CAPTURE_SCRIPT" \
    RUN="$RUN" TS="$TS" N="$N" IFACE_USE="$IFACE_USE" DUR_CAPTURE="$DUR_CAPTURE" \
    >"$RUN/bg_capture.log" 2>&1 &
  echo $! > "$RUN/bg_capture.pid"
  echo "[INFO] PID=$(cat "$RUN/bg_capture.pid")  LOG=$RUN/bg_capture.log"
}

if [[ ! -x "$CAPTURE_SCRIPT" && ! -f "$CAPTURE_SCRIPT" ]]; then
  die "Capture script not found: $CAPTURE_SCRIPT"
fi

case "$MODE" in
  fg) run_capture_fg ;;
  bg) run_capture_bg ;;
  *) die "MODE must be fg or bg (got: $MODE)" ;;
esac

# -------- normalize phases location --------
echo
echo "[INFO] Normalizing phases to canonical path..."

# Common places where phases may appear
# 1) RUN/<phases_name>          (desired)
# 2) LAB_FLOWS/<phases_name>    (legacy)
# 3) RUN/phases_safe_multi      (if PHASES_NAME differs)
# 4) LAB_FLOWS/phases_safe_multi (legacy)
move_into_canon_if_needed "$COMPAT_PHASES" "$CANON_PHASES"
move_into_canon_if_needed "$RUN/phases_safe_multi" "$CANON_PHASES"
move_into_canon_if_needed "$LAB_FLOWS/phases_safe_multi" "$CANON_PHASES"
move_into_canon_if_needed "$RUN/phases_safe" "$RUN/phases_safe"  # no-op safety

# Verify CSVs exist
c_can="$(csv_count "$CANON_PHASES")"
if [[ "$c_can" -eq 0 ]]; then
  echo "[DEBUG] Listing candidate dirs:"
  (ls -lah "$RUN" || true) | sed 's/^/[DEBUG] /'
  (ls -lah "$LAB_FLOWS" || true) | sed 's/^/[DEBUG] /'
  die "csv_count=0 in canonical phases dir: $CANON_PHASES"
fi

# Enforce multi-attack expectation if requested
if [[ "$REQUIRE_MULTI" -eq 1 && "$PHASES_NAME" == "phases_safe_multi" ]]; then
  assert_expected_multi "$CANON_PHASES"
fi

# Create compatibility symlink (downstream scripts expecting lab/flows path)
ensure_symlink_compat "$CANON_PHASES" "$COMPAT_PHASES"

echo "[INFO] Canonical phases CSV count: $c_can"
echo

# -------- optional: artifact bundle skeleton --------
if [[ "$DO_BUNDLE" -eq 1 ]]; then
  echo "[INFO] Creating lightweight bundle (logs + phases pointers)..."
  BUNDLE="$BUNDLES/mycelium_run_${TS}.zip"

  # Include logs and phase CSVs. Avoid massive pcaps unless you want them.
  # Adjust patterns as needed.
  (cd "$RUN" && zip -r "$BUNDLE" \
      ./*.log ./*.pid \
      "$PHASES_NAME" \
      -x "*.pcap" "*.pcapng" "*/_phase_*.pcap" \
    ) >/dev/null || true

  echo "[INFO] Bundle created: $BUNDLE"
fi

echo
echo "[DONE] Normalization complete."
echo "       RUN:          $RUN"
echo "       CANON_PHASES: $CANON_PHASES"
echo "       COMPAT_LINK:  $COMPAT_PHASES -> $CANON_PHASES"
