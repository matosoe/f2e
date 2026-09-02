#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
build="$root/.build"
mkdir -p "$build/organizer" "$build/worker"

cd "$root/lambdas"
GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-buildid=' -o "$build/organizer/bootstrap" ./cmd/organizer
GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-buildid=' -o "$build/worker/bootstrap" ./cmd/worker

cd "$root"
go run ./automacao/cmd/package "$build"
