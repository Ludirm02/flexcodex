#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT="${1:-9000}"
ROWS="${2:-10000000}"
OUT_FILE="${3:-$ROOT_DIR/docs/performance_results.txt}"

cd "$ROOT_DIR"
make -j"$(nproc)" >/dev/null

SERVER_LOG="$(mktemp -t flexql_server_log.XXXXXX)"
"$ROOT_DIR/build/flexql_server" "$PORT" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

cleanup() {
  if kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -f "$SERVER_LOG"
}
trap cleanup EXIT

sleep 1

"$ROOT_DIR/build/flexql_benchmark" 127.0.0.1 "$PORT" "$ROWS" | tee "$OUT_FILE"

echo "Saved benchmark output to $OUT_FILE"
