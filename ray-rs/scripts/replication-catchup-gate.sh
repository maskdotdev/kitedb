#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/../docs/benchmarks/results}"

SEED_COMMITS="${SEED_COMMITS:-1000}"
BACKLOG_COMMITS="${BACKLOG_COMMITS:-5000}"
MAX_FRAMES="${MAX_FRAMES:-256}"
SYNC_MODE="${SYNC_MODE:-normal}"
SEGMENT_MAX_BYTES="${SEGMENT_MAX_BYTES:-67108864}"
RETENTION_MIN="${RETENTION_MIN:-20000}"
MIN_CATCHUP_FPS="${MIN_CATCHUP_FPS:-2000}"
MIN_THROUGHPUT_RATIO="${MIN_THROUGHPUT_RATIO:-0.09}"
ATTEMPTS="${ATTEMPTS:-3}"

if [[ "$BACKLOG_COMMITS" -lt 100 ]]; then
  echo "BACKLOG_COMMITS must be >= 100 for stable catch-up measurements"
  exit 1
fi
if [[ "$ATTEMPTS" -lt 1 ]]; then
  echo "ATTEMPTS must be >= 1"
  exit 1
fi

mkdir -p "$OUT_DIR"
STAMP="${STAMP:-$(date +%F)}"
LOGFILE_BASE="$OUT_DIR/${STAMP}-replication-catchup-gate"

best_catchup_fps=0
best_ratio=0
best_logfile=""
last_logfile=""
last_catchup_fps=""
last_primary_fps=""
last_ratio=""
last_applied_frames=""

run_once() {
  local logfile="$1"
  (
    cd "$ROOT_DIR"
    cargo run --release --example replication_catchup_bench --no-default-features -- \
      --seed-commits "$SEED_COMMITS" \
      --backlog-commits "$BACKLOG_COMMITS" \
      --max-frames "$MAX_FRAMES" \
      --sync-mode "$SYNC_MODE" \
      --segment-max-bytes "$SEGMENT_MAX_BYTES" \
      --retention-min "$RETENTION_MIN" >"$logfile"
  )
}

echo "== Replication catch-up gate (attempts: $ATTEMPTS)"
for attempt in $(seq 1 "$ATTEMPTS"); do
  if [[ "$ATTEMPTS" -eq 1 ]]; then
    logfile="${LOGFILE_BASE}.txt"
  else
    logfile="${LOGFILE_BASE}.attempt${attempt}.txt"
  fi

  run_once "$logfile"

  catchup_fps="$(grep '^catchup_frames_per_sec:' "$logfile" | tail -1 | awk '{print $2}')"
  primary_fps="$(grep '^primary_frames_per_sec:' "$logfile" | tail -1 | awk '{print $2}')"
  ratio="$(grep '^throughput_ratio:' "$logfile" | tail -1 | awk '{print $2}')"
  applied_frames="$(grep '^applied_frames:' "$logfile" | tail -1 | awk '{print $2}')"

  if [[ -z "$catchup_fps" || -z "$primary_fps" || -z "$ratio" || -z "$applied_frames" ]]; then
    echo "failed: could not parse catch-up metrics from benchmark output"
    echo "log: $logfile"
    exit 1
  fi

  last_logfile="$logfile"
  last_catchup_fps="$catchup_fps"
  last_primary_fps="$primary_fps"
  last_ratio="$ratio"
  last_applied_frames="$applied_frames"

  if awk -v current="$catchup_fps" -v best="$best_catchup_fps" 'BEGIN { exit !(current > best) }'; then
    best_catchup_fps="$catchup_fps"
    best_ratio="$ratio"
    best_logfile="$logfile"
  fi

  fps_pass="$(awk -v actual="$catchup_fps" -v min="$MIN_CATCHUP_FPS" 'BEGIN { if (actual >= min) print "yes"; else print "no" }')"
  ratio_pass="$(awk -v actual="$ratio" -v min="$MIN_THROUGHPUT_RATIO" 'BEGIN { if (actual >= min) print "yes"; else print "no" }')"

  echo "attempt $attempt/$ATTEMPTS: applied=$applied_frames primary_fps=$primary_fps catchup_fps=$catchup_fps ratio=$ratio"

  if [[ "$fps_pass" == "yes" && "$ratio_pass" == "yes" ]]; then
    echo "pass: replication catch-up throughput gate satisfied"
    echo "log:"
    echo "  $logfile"
    exit 0
  fi
done

echo "failed: catch-up throughput gate did not pass in $ATTEMPTS attempt(s)"
echo "last attempt: applied frames=$last_applied_frames primary frames/sec=$last_primary_fps catchup frames/sec=$last_catchup_fps ratio=$last_ratio"
echo "thresholds: catchup_fps >= $MIN_CATCHUP_FPS, ratio >= $MIN_THROUGHPUT_RATIO"
if [[ -n "$best_logfile" ]]; then
  echo "best attempt: catchup_fps=$best_catchup_fps ratio=$best_ratio log=$best_logfile"
fi
echo "last log:"
echo "  $last_logfile"
exit 1
