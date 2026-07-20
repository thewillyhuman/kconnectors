#!/usr/bin/env bash
# Prints, as a compact JSON array, the connectors affected by a commit range:
#
#   tools/ci/affected-connectors.sh <base> [<head>]      (head defaults to HEAD)
#
# A connector is affected when files under connectors/<name>/ changed. A change
# to anything shared between connectors (MODULE.bazel, maven_install.json,
# .bazelrc, tools/, workflows, ...) affects every connector, because it can
# change how every connector builds. Top-level documentation affects nothing.
# When the base commit is unknown (new branch, force push, shallow clone) every
# connector is considered affected: over-building is safe, skipping is not.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repo_root"

base="${1:-}"
head="${2:-HEAD}"

all_connectors() {
  find connectors -mindepth 1 -maxdepth 1 -type d | sed 's|^connectors/||' | sort
}

as_json() {
  local out="[" first=1 name
  while IFS= read -r name; do
    [[ -z "$name" ]] && continue
    [[ $first -eq 0 ]] && out+=","
    out+="\"$name\""
    first=0
  done
  printf '%s]\n' "$out"
}

if [[ -z "$base" ]] || ! git rev-parse -q --verify "$base^{commit}" >/dev/null 2>&1; then
  echo "No usable base commit ('${base:-<empty>}'); every connector is affected." >&2
  all_connectors | as_json
  exit 0
fi

# Three-dot diff: changes on head's side since the merge base, which is what a
# pull request will actually merge.
changed="$(git diff --name-only "$base...$head")"

if [[ -z "$changed" ]]; then
  echo "No changes between $base and $head." >&2
  echo "[]"
  exit 0
fi

affected=""
shared_change=""
while IFS= read -r file; do
  case "$file" in
    connectors/*/*)
      name="${file#connectors/}"
      name="${name%%/*}"
      affected+="$name"$'\n'
      ;;
    *.md | LICENSE | NOTICE | .gitignore)
      ;;
    *)
      shared_change="$file"
      ;;
  esac
done <<<"$changed"

if [[ -n "$shared_change" ]]; then
  echo "Shared file changed ($shared_change); every connector is affected." >&2
  all_connectors | as_json
  exit 0
fi

# Deduplicate and drop connectors that no longer exist (e.g. just deleted).
printf '%s' "$affected" | sort -u | while IFS= read -r name; do
  [[ -d "connectors/$name" ]] && printf '%s\n' "$name" || true
done | as_json
