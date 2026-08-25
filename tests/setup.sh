#!/usr/bin/env bash
set -e

script_dir=$(dirname -- "$(readlink -f -- "$0")")
repo_dir="$(cd "$script_dir/.." && pwd)"

echo "Compiling circular plugin from source..."
cd "$repo_dir"
go build -o circular ./cmd/circular
chmod +x circular
