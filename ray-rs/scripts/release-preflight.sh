#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RAY_RS_DIR="$ROOT_DIR/ray-rs"

usage() {
  cat <<USAGE
Usage:
  ray-rs/scripts/release-preflight.sh [--commit-msg "all|js|ts|py|rs|core: X.Y.Z"] [--tag vX.Y.Z]

Behavior:
  - Validates release commit message format against AGENTS.md rules.
  - In strict mode, extra text after version fails preflight.
  - If --tag is provided, validates tag format and ray-rs/package.json version match.
USAGE
}

commit_msg=""
tag=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --commit-msg)
      commit_msg="${2:-}"
      shift 2
      ;;
    --tag)
      tag="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$commit_msg" ]]; then
  commit_msg="$(git -C "$ROOT_DIR" log -1 --pretty=%B | head -n 1)"
fi

if [[ -z "$commit_msg" ]]; then
  echo "failed: empty commit message"
  exit 1
fi

# Strict stable release pattern (no trailing text).
release_re='^(all|js|ts|py|rs|core): ([0-9]+)\.([0-9]+)\.([0-9]+)$'
# Permissive pattern used by CI routing (extra text -> npm next).
routing_re='^(all|js|ts|py|rs|core): ([0-9]+)\.([0-9]+)\.([0-9]+)( .+)?$'

if [[ "$commit_msg" =~ $release_re ]]; then
  channel="${BASH_REMATCH[1]}"
  version="${BASH_REMATCH[2]}.${BASH_REMATCH[3]}.${BASH_REMATCH[4]}"
  echo "ok: strict release commit message"
  echo "  channel=$channel version=$version"
elif [[ "$commit_msg" =~ $routing_re ]]; then
  channel="${BASH_REMATCH[1]}"
  version="${BASH_REMATCH[2]}.${BASH_REMATCH[3]}.${BASH_REMATCH[4]}"
  echo "failed: commit message has trailing text; this routes npm to next"
  echo "  message=$commit_msg"
  echo "  expected exact format: all|js|ts|py|rs|core: X.Y.Z"
  exit 1
else
  echo "failed: commit message does not match release-gate format"
  echo "  message=$commit_msg"
  echo "  expected exact format: all|js|ts|py|rs|core: X.Y.Z"
  exit 1
fi

if [[ -n "$tag" ]]; then
  if [[ ! "$tag" =~ ^v([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
    echo "failed: tag must match vX.Y.Z"
    echo "  tag=$tag"
    exit 1
  fi

  tag_version="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}.${BASH_REMATCH[3]}"

  package_version="$(node -e "const p=require('$RAY_RS_DIR/package.json');process.stdout.write(String(p.version||''))")"
  if [[ -z "$package_version" ]]; then
    echo "failed: could not read ray-rs/package.json version"
    exit 1
  fi

  if [[ "$package_version" != "$tag_version" ]]; then
    echo "failed: ray-rs/package.json version does not match tag"
    echo "  package.json=$package_version"
    echo "  tag=$tag"
    exit 1
  fi

  if [[ "$version" != "$tag_version" ]]; then
    echo "failed: commit message version does not match tag"
    echo "  commit=$version"
    echo "  tag=$tag"
    exit 1
  fi

  echo "ok: tag + package version + commit version aligned"
  echo "  tag=$tag"
fi

echo "pass: release preflight checks satisfied"
