#!/usr/bin/env bash
# Lints one connector's Java sources with PMD.
#
#   tools/lint/pmd.sh <connector-name>          e.g. kafka-connector-source-amq
#
# Downloads the pinned PMD distribution into PMD_CACHE_DIR (default:
# ~/.cache/kconnectors-pmd) on first use; CI persists that directory between
# runs. Exits non-zero on violations, which makes the lint job blocking.
set -euo pipefail

PMD_VERSION="7.21.0"
PMD_CACHE_DIR="${PMD_CACHE_DIR:-$HOME/.cache/kconnectors-pmd}"

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
connector="${1:?usage: pmd.sh <connector-name>}"
connector_dir="$repo_root/connectors/$connector"

if [[ ! -d "$connector_dir/src" ]]; then
  echo "error: no such connector: $connector_dir" >&2
  exit 2
fi

pmd_home="$PMD_CACHE_DIR/pmd-bin-$PMD_VERSION"
if [[ ! -x "$pmd_home/bin/pmd" ]]; then
  echo "Fetching PMD $PMD_VERSION into $PMD_CACHE_DIR"
  mkdir -p "$PMD_CACHE_DIR"
  curl -sfL -o "$PMD_CACHE_DIR/pmd.zip" \
    "https://github.com/pmd/pmd/releases/download/pmd_releases%2F${PMD_VERSION}/pmd-dist-${PMD_VERSION}-bin.zip"
  unzip -q -o "$PMD_CACHE_DIR/pmd.zip" -d "$PMD_CACHE_DIR"
  rm -f "$PMD_CACHE_DIR/pmd.zip"
fi

exec "$pmd_home/bin/pmd" check \
  --dir "$connector_dir/src" \
  --rulesets "$repo_root/tools/lint/pmd-ruleset.xml" \
  --format text \
  --no-cache \
  --no-progress
