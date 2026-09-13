#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
mkdir -p dist
for arch in arm64 amd64; do
  CGO_ENABLED=0 GOOS=darwin GOARCH="$arch" go build -trimpath -ldflags='-s -w' -o "dist/kaflow-darwin-$arch" .
done
cp "dist/kaflow-darwin-$(go env GOARCH)" dist/kaflow
printf 'Built Apple Silicon and Intel binaries in dist/\n'
