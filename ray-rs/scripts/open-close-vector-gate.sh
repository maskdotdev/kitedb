#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/../docs/benchmarks/results}"

ATTEMPTS="${ATTEMPTS:-1}"
MAX_SMALL_RW_RATIO="${MAX_SMALL_RW_RATIO:-5.0}"
MAX_SMALL_RO_RATIO="${MAX_SMALL_RO_RATIO:-5.0}"
MAX_LARGE_RW_RATIO="${MAX_LARGE_RW_RATIO:-2.5}"
MAX_LARGE_RO_RATIO="${MAX_LARGE_RO_RATIO:-2.5}"

if [[ "$ATTEMPTS" -lt 1 ]]; then
  echo "ATTEMPTS must be >= 1"
  exit 1
fi

mkdir -p "$OUT_DIR"
STAMP="${STAMP:-$(date +%F)}"
LOG_BASE="$OUT_DIR/${STAMP}-open-close-vector-gate"
BENCH_FILTER='single_file_open_close/open_close/(rw|ro)/graph_10k_20k(_vec5k)?$|single_file_open_close_limits/open_close/(rw|ro)/graph_100k_200k(_vec20k)?$'

extract_median_us() {
  local logfile="$1"
  local bench_id="$2"
  local line
  line="$(
    awk -v bench_id="$bench_id" '
      $0 == bench_id { in_block = 1; next }
      in_block && $1 == "time:" { print; exit }
    ' "$logfile"
  )"
  if [[ -z "$line" ]]; then
    return 1
  fi

  local value unit
  value="$(awk '{print $4}' <<<"$line")"
  unit="$(awk '{print $5}' <<<"$line")"
  unit="${unit//]/}"

  awk -v value="$value" -v unit="$unit" 'BEGIN {
    if (unit == "ns") {
      printf "%.6f", value / 1000.0
    } else if (unit == "us" || unit == "µs") {
      printf "%.6f", value + 0.0
    } else if (unit == "ms") {
      printf "%.6f", value * 1000.0
    } else if (unit == "s") {
      printf "%.6f", value * 1000000.0
    } else {
      exit 1
    }
  }'
}

median() {
  printf '%s\n' "$@" | sort -g | awk '
    {
      a[NR] = $1
    }
    END {
      if (NR == 0) {
        print "NaN"
      } else if (NR % 2 == 1) {
        printf "%.6f", a[(NR + 1) / 2]
      } else {
        printf "%.6f", (a[NR / 2] + a[NR / 2 + 1]) / 2
      }
    }
  '
}

declare -a small_rw_ratios=()
declare -a small_ro_ratios=()
declare -a large_rw_ratios=()
declare -a large_ro_ratios=()
last_log=""

echo "== Open/close vector gate (attempts: $ATTEMPTS)"
for attempt in $(seq 1 "$ATTEMPTS"); do
  if [[ "$ATTEMPTS" -eq 1 ]]; then
    logfile="${LOG_BASE}.txt"
  else
    logfile="${LOG_BASE}.attempt${attempt}.txt"
  fi
  last_log="$logfile"

  (
    cd "$ROOT_DIR"
    cargo bench --bench single_file --no-default-features -- "$BENCH_FILTER" >"$logfile"
  )

  small_rw_base_us="$(extract_median_us "$logfile" "single_file_open_close/open_close/rw/graph_10k_20k")"
  small_rw_vec_us="$(extract_median_us "$logfile" "single_file_open_close/open_close/rw/graph_10k_20k_vec5k")"
  small_ro_base_us="$(extract_median_us "$logfile" "single_file_open_close/open_close/ro/graph_10k_20k")"
  small_ro_vec_us="$(extract_median_us "$logfile" "single_file_open_close/open_close/ro/graph_10k_20k_vec5k")"
  large_rw_base_us="$(extract_median_us "$logfile" "single_file_open_close_limits/open_close/rw/graph_100k_200k")"
  large_rw_vec_us="$(extract_median_us "$logfile" "single_file_open_close_limits/open_close/rw/graph_100k_200k_vec20k")"
  large_ro_base_us="$(extract_median_us "$logfile" "single_file_open_close_limits/open_close/ro/graph_100k_200k")"
  large_ro_vec_us="$(extract_median_us "$logfile" "single_file_open_close_limits/open_close/ro/graph_100k_200k_vec20k")"

  if [[ -z "$small_rw_base_us" || -z "$small_rw_vec_us" || -z "$small_ro_base_us" || -z "$small_ro_vec_us" || -z "$large_rw_base_us" || -z "$large_rw_vec_us" || -z "$large_ro_base_us" || -z "$large_ro_vec_us" ]]; then
    echo "failed: could not parse one or more open/close benchmark medians"
    echo "log: $logfile"
    exit 1
  fi

  ratio_small_rw="$(awk -v base="$small_rw_base_us" -v vec="$small_rw_vec_us" 'BEGIN { printf "%.6f", vec / base }')"
  ratio_small_ro="$(awk -v base="$small_ro_base_us" -v vec="$small_ro_vec_us" 'BEGIN { printf "%.6f", vec / base }')"
  ratio_large_rw="$(awk -v base="$large_rw_base_us" -v vec="$large_rw_vec_us" 'BEGIN { printf "%.6f", vec / base }')"
  ratio_large_ro="$(awk -v base="$large_ro_base_us" -v vec="$large_ro_vec_us" 'BEGIN { printf "%.6f", vec / base }')"

  small_rw_ratios+=("$ratio_small_rw")
  small_ro_ratios+=("$ratio_small_ro")
  large_rw_ratios+=("$ratio_large_rw")
  large_ro_ratios+=("$ratio_large_ro")

  echo "attempt $attempt/$ATTEMPTS:"
  echo "  small-rw ratio(vec/non-vec) = $ratio_small_rw"
  echo "  small-ro ratio(vec/non-vec) = $ratio_small_ro"
  echo "  large-rw ratio(vec/non-vec) = $ratio_large_rw"
  echo "  large-ro ratio(vec/non-vec) = $ratio_large_ro"
done

median_small_rw="$(median "${small_rw_ratios[@]}")"
median_small_ro="$(median "${small_ro_ratios[@]}")"
median_large_rw="$(median "${large_rw_ratios[@]}")"
median_large_ro="$(median "${large_ro_ratios[@]}")"

if [[ "$median_small_rw" == "NaN" || "$median_small_ro" == "NaN" || "$median_large_rw" == "NaN" || "$median_large_ro" == "NaN" ]]; then
  echo "failed: no ratios captured"
  exit 1
fi

small_rw_pass="$(awk -v actual="$median_small_rw" -v max="$MAX_SMALL_RW_RATIO" 'BEGIN { if (actual <= max) print "yes"; else print "no" }')"
small_ro_pass="$(awk -v actual="$median_small_ro" -v max="$MAX_SMALL_RO_RATIO" 'BEGIN { if (actual <= max) print "yes"; else print "no" }')"
large_rw_pass="$(awk -v actual="$median_large_rw" -v max="$MAX_LARGE_RW_RATIO" 'BEGIN { if (actual <= max) print "yes"; else print "no" }')"
large_ro_pass="$(awk -v actual="$median_large_ro" -v max="$MAX_LARGE_RO_RATIO" 'BEGIN { if (actual <= max) print "yes"; else print "no" }')"

echo "median small-rw ratio across $ATTEMPTS attempt(s): $median_small_rw (max allowed: $MAX_SMALL_RW_RATIO)"
echo "median small-ro ratio across $ATTEMPTS attempt(s): $median_small_ro (max allowed: $MAX_SMALL_RO_RATIO)"
echo "median large-rw ratio across $ATTEMPTS attempt(s): $median_large_rw (max allowed: $MAX_LARGE_RW_RATIO)"
echo "median large-ro ratio across $ATTEMPTS attempt(s): $median_large_ro (max allowed: $MAX_LARGE_RO_RATIO)"
echo "log: $last_log"

if [[ "$small_rw_pass" != "yes" || "$small_ro_pass" != "yes" || "$large_rw_pass" != "yes" || "$large_ro_pass" != "yes" ]]; then
  echo "failed: open/close vector gate not satisfied"
  exit 1
fi

echo "pass: open/close vector gate satisfied"
