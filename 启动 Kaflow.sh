#!/bin/sh
set -eu
cd "$(dirname "$0")"
exec ./dist/kaflow-linux-$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/;s/arm64/arm64/')
