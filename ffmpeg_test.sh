#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v ffmpeg >/dev/null 2>&1; then
  echo "ffmpeg not found in PATH"
  exit 1
fi

echo "building test server"
go build -o /tmp/rtmp-testserver ./cmd/testserver

if [ ! -f testdata/sample.mp4 ]; then
  echo "sample.mp4 missing, generating a short sample"
  ffmpeg -y \
    -f lavfi -i testsrc=duration=10:size=640x360:rate=30 \
    -f lavfi -i sine=frequency=1000:duration=10 \
    -c:v libx264 -pix_fmt yuv420p \
    -c:a aac \
    testdata/sample.mp4
fi

echo "starting test server"
/tmp/rtmp-testserver -port 1935 &
SERVER_PID=$!
cleanup() {
  kill "${SERVER_PID}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

sleep 0.5

echo "publishing sample.mp4 via ffmpeg"
ffmpeg -re -i testdata/sample.mp4 \
  -c:v libx264 -preset ultrafast -tune zerolatency \
  -c:a aac \
  -f flv rtmp://127.0.0.1:1935/live/testkey

wait "${SERVER_PID}"
