#!/usr/bin/env bash
# End-to-end demonstration of every scenario required by the specification
# (section 10). Start the cluster first (`docker compose up -d --build`).
#
#   scripts/demo.sh [all|storage|chunking|replication|metadata|failure|integrity]
#
# Environment:
#   OBJSTORE_URL   controller URL              (default http://localhost:8080)
#   OBJSTORE_MODE  docker | local              (default docker)
#   DEMO_PAUSE=1   wait for <enter> between steps (for live presentations)
set -euo pipefail
cd "$(dirname "$0")/.."

export OBJSTORE_URL=${OBJSTORE_URL:-http://localhost:8080}
MODE=${OBJSTORE_MODE:-docker}
WORK=demo-data
mkdir -p "$WORK"

BOLD=$'\e[1m'; CYAN=$'\e[36m'; GREEN=$'\e[32m'; RESET=$'\e[0m'

cli() { python3 client/objstore_cli.py "$@"; }
banner() { printf '\n%s======================================================================\n %s\n======================================================================%s\n' "$BOLD" "$1" "$RESET"; }
say() { printf '\n%s# %s%s\n' "$CYAN" "$*" "$RESET"; }
run() { printf '%s$ %s%s\n' "$GREEN" "$*" "$RESET"; "$@"; }
pause() { if [ "${DEMO_PAUSE:-0}" = 1 ]; then read -rp "  [press enter to continue]" _; fi; }
make_file() { python3 -c 'import os,sys; open(sys.argv[1],"wb").write(os.urandom(int(sys.argv[2])))' "$1" "$2"; }

stop_worker() {
  if [ "$MODE" = local ]; then run scripts/local_cluster.sh stop-worker "$1"; else run docker stop "$1"; fi
}
start_worker() {
  if [ "$MODE" = local ]; then run scripts/local_cluster.sh start-worker "$1"; else run docker start "$1"; fi
}

wait_for_cluster() {
  say "Waiting for the controller and 4 UP workers at $OBJSTORE_URL"
  for _ in $(seq 1 60); do
    if cli --json cluster 2>/dev/null | python3 -c 'import json,sys; sys.exit(0 if json.load(sys.stdin)["workers_up"] >= 4 else 1)' 2>/dev/null; then
      cli cluster; return 0
    fi
    sleep 2
  done
  echo "cluster did not become ready"; exit 1
}

# ---------------------------------------------------------------------------

demo_storage() {
  banner "1. Object storage: upload, retrieve, verify"
  printf 'Hello from the Mini Distributed Cloud Object Store!\n' > "$WORK/hello.txt"
  say "Upload a small text document"
  OID=$(cli upload "$WORK/hello.txt" -q)
  echo "object id: $OID"
  say "Download it and compare with the original"
  run cli download "$OID" -o "$WORK/hello.downloaded.txt"
  run cmp "$WORK/hello.txt" "$WORK/hello.downloaded.txt" && echo "cmp: files are byte-for-byte identical"
  run cli verify "$OID" "$WORK/hello.txt"
  pause
}

demo_chunking() {
  banner "2. Chunking and distribution (object > 1 MB)"
  make_file "$WORK/big.bin" $((5 * 1024 * 1024 + 300 * 1024))
  say "Upload a 5.3 MB binary object -> 6 chunks (5 x 1 MB + 300 KB)"
  BIG=$(cli upload "$WORK/big.bin" -q -m purpose=chunking-demo)
  echo "object id: $BIG"
  run cli locate "$BIG"
  say "Physical chunk files reported by each worker"
  run cli workers
  run cli verify "$BIG" "$WORK/big.bin"
  pause
}

demo_replication() {
  banner "3. Replication: every chunk has 2 copies on different workers"
  [ -n "${BIG:-}" ] || { make_file "$WORK/big.bin" $((3 * 1024 * 1024)); BIG=$(cli upload "$WORK/big.bin" -q); }
  run cli locate "$BIG"
  say "Controller-wide replication summary"
  run cli cluster
  pause
}

demo_metadata() {
  banner "4. Metadata: custom key/values, retrieve and modify without re-upload"
  make_file "$WORK/report.pdf" 1500000
  say "Upload with custom metadata"
  printf '%s$ cli upload %s -m owner=alice -m department=research -m confidential=true%s\n' "$GREEN" "$WORK/report.pdf" "$RESET"
  MOID=$(cli upload "$WORK/report.pdf" -m owner=alice -m department=research -m confidential=true -q)
  echo "object id: $MOID"
  say "Retrieve metadata only (no object data is transferred)"
  run cli info "$MOID"
  say "Modify metadata: change owner, add reviewed, remove department"
  run cli set-meta "$MOID" owner=bob reviewed=2026-10-18 --remove department
  say "Optimistic concurrency: an update with a stale If-Match version is rejected (409)"
  run cli set-meta "$MOID" owner=mallory --if-match 1 || echo "(rejected as expected)"
  say "Data is unchanged"
  run cli verify "$MOID" "$WORK/report.pdf"
  say "Raw REST call for reference"
  run curl -s -X PATCH -H 'Content-Type: application/json' -d '{"tag":"via-curl"}' \
      "$OBJSTORE_URL/api/v1/objects/$MOID/metadata?include_chunks=false"
  echo
  pause
}

demo_failure() {
  banner "5. Worker failure: docker stop worker2 / docker start worker2"
  make_file "$WORK/failure.bin" $((6 * 1024 * 1024))
  FOID=$(cli upload "$WORK/failure.bin" -q)
  say "BEFORE: all workers UP, every chunk has 2 copies"
  run cli locate "$FOID"
  run cli cluster
  pause

  say "Stop worker2"
  stop_worker worker2
  say "The controller notices missing heartbeats (timeout 10 s) and marks worker2 DOWN"
  run cli wait-worker worker2 DOWN --timeout 60
  say "DURING: chunks that had a copy on worker2 are under-replicated"
  run cli locate "$FOID"
  run cli cluster
  say "The object is still fully readable from the remaining copies"
  run cli verify "$FOID" "$WORK/failure.bin"
  say "Uploads keep working with 3 workers"
  printf 'written while worker2 was down\n' > "$WORK/during.txt"
  run cli upload "$WORK/during.txt" -q
  pause

  say "After the grace period the controller re-replicates the missing copies"
  run cli wait-healthy --timeout 120
  run cli locate "$FOID"
  run cli events --limit 15
  pause

  say "Restart worker2 (its persistent volume still has the old copies)"
  start_worker worker2
  run cli wait-worker worker2 UP --timeout 60
  say "AFTER: worker2 is reconciled; surplus third copies are trimmed back to exactly 2"
  run cli wait-healthy "$FOID" --timeout 60
  run cli locate "$FOID"
  run cli cluster
  run cli events --limit 10
  run cli verify "$FOID" "$WORK/failure.bin"
  pause
}

demo_integrity() {
  banner "6. Data integrity: intentional corruption"
  make_file "$WORK/integrity.bin" $((3 * 1024 * 1024 + 4096))
  IOID=$(cli upload "$WORK/integrity.bin" -q)
  run cli locate "$IOID"

  read -r CHUNK W1 W2 <<<"$(cli locate "$IOID" --chunk 1)"
  say "Corrupt the copy of chunk 1 ($CHUNK) stored on $W1 (the copy on $W2 stays intact)"
  run scripts/corrupt_chunk.sh "$W1" "$CHUNK" flip
  say "Downloads verify every chunk's SHA-256 and never return the corrupted bytes"
  for i in 1 2 3; do run cli verify "$IOID" "$WORK/integrity.bin"; done
  say "Run an integrity scrub (also happens on reads, worker scrubs and worker rejoin)"
  run cli scrub >/dev/null
  run cli events --level error --limit 5
  say "The corrupted copy was restored in place from the healthy copy"
  run cli wait-healthy "$IOID" --timeout 60
  run cli locate "$IOID"
  pause

  read -r CHUNK2 W3 _ <<<"$(cli locate "$IOID" --chunk 2)"
  say "Delete a chunk file outright on $W3 (simulated lost data) and truncate another"
  run scripts/corrupt_chunk.sh "$W3" "$CHUNK2" delete
  read -r CHUNK0 _ W4 <<<"$(cli locate "$IOID" --chunk 0)"
  run scripts/corrupt_chunk.sh "$W4" "$CHUNK0" truncate
  run cli verify "$IOID" "$WORK/integrity.bin"
  run cli scrub >/dev/null
  run cli wait-healthy "$IOID" --timeout 60
  run cli locate "$IOID"
  run cli events --limit 8
}

case "${1:-all}" in
  all)
    wait_for_cluster
    demo_storage; demo_chunking; demo_replication; demo_metadata; demo_failure; demo_integrity
    banner "Demo complete"
    run cli list
    run cli cluster ;;
  storage|chunking|replication|metadata|failure|integrity)
    wait_for_cluster
    "demo_$1" ;;
  *) sed -n '2,12p' "$0"; exit 1 ;;
esac
