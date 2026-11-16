#!/usr/bin/env bash
# Intentionally damage one stored copy of a chunk (data-integrity demo).
#
#   scripts/corrupt_chunk.sh WORKER CHUNK_ID [flip|truncate|delete]
#
#   flip      overwrite 16 bytes in the middle of the chunk (default)
#   truncate  cut the chunk file in half
#   delete    remove the chunk file
#
# Works against the Docker deployment (default) or the local cluster
# (OBJSTORE_MODE=local, see scripts/local_cluster.sh).
set -euo pipefail
cd "$(dirname "$0")/.."

WORKER=${1:?usage: corrupt_chunk.sh WORKER CHUNK_ID [flip|truncate|delete]}
CHUNK=${2:?missing CHUNK_ID}
ACTION=${3:-flip}
MODE=${OBJSTORE_MODE:-docker}

read -r -d '' PY <<'PYEOF' || true
import os, sys
path, action = sys.argv[1], sys.argv[2]
size = os.path.getsize(path)
if action == "flip":
    with open(path, "r+b") as fh:
        offset = size // 2
        fh.seek(offset)
        original = fh.read(16)
        fh.seek(offset)
        fh.write(bytes(b ^ 0xFF for b in original))
    print(f"flipped {len(original)} bytes at offset {offset} of {path} ({size} bytes)")
elif action == "truncate":
    with open(path, "r+b") as fh:
        fh.truncate(size // 2)
    print(f"truncated {path} from {size} to {size // 2} bytes")
elif action == "delete":
    os.remove(path)
    print(f"deleted {path}")
else:
    sys.exit(f"unknown action {action}")
PYEOF

if [ "$MODE" = "local" ]; then
  python3 -c "$PY" "${LOCAL_DATA_DIR:-.local-data}/$WORKER/chunks/$CHUNK.chunk" "$ACTION"
else
  docker exec "$WORKER" python -c "$PY" "/data/chunks/$CHUNK.chunk" "$ACTION"
fi
