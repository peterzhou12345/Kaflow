#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
mkdir -p dist

build() {
  os="$1"; arch="$2"; suffix=""
  [ "$os" = windows ] && suffix=".exe"
  printf 'Building %s/%s...\n' "$os" "$arch"
  CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go build -trimpath -ldflags='-s -w' -o "dist/kaflow-${os}-${arch}${suffix}" .
}

build darwin arm64
build darwin amd64
build windows amd64
build linux amd64
build linux arm64
cp "dist/kaflow-darwin-$(go env GOARCH)" dist/kaflow
printf 'Built macOS, Windows and Linux binaries in dist/.\n'
