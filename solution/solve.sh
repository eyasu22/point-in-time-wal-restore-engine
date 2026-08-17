#!/usr/bin/env bash
set -euo pipefail

install_from() {
  local src="$1" dst="$2"
  [[ -d "$src" ]] || { echo "missing reference $src" >&2; return 1; }
  rm -rf "$dst"
  mkdir -p "$dst"
  cp -a "$src"/. "$dst"/
  echo "Reference solution installed into $dst"
}

if [[ -d /solution/reference/app && -d /app ]]; then
  install_from /solution/reference/app /app/app
  exit 0
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
install_from "$ROOT/solution/reference/app" "$ROOT/app"
