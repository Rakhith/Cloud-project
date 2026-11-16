#!/usr/bin/env bash
# Run the whole cluster as local processes (no Docker) for development and
# quick experiments. Ports: controller 8080, workers 9001-9004.
#
#   scripts/local_cluster.sh up              start controller + 4 workers
#   scripts/local_cluster.sh down            stop everything
#   scripts/local_cluster.sh stop-worker 2   simulate `docker stop worker2`
#   scripts/local_cluster.sh start-worker 2  simulate `docker start worker2`
#   scripts/local_cluster.sh status | logs [name] | clean
#
# Requires the Python dependencies: pip install -r requirements-dev.txt uvicorn
set -euo pipefail
cd "$(dirname "$0")/.."

PY=${PYTHON:-$( [ -x .venv/bin/python ] && echo .venv/bin/python || echo python3 )}
DATA=${LOCAL_DATA_DIR:-.local-data}
RUN=$DATA/run
# Settings chosen at `up` are remembered so later start-worker calls match.
if [ -z "${CONTROLLER_PORT:-}" ] && [ -f "$DATA/run/ports.env" ]; then . "$DATA/run/ports.env"; fi
CONTROLLER_PORT=${CONTROLLER_PORT:-8080}
TOKEN=${INTERNAL_TOKEN:-local-token}
mkdir -p "$RUN"

is_running() { [ -f "$RUN/$1.pid" ] && kill -0 "$(cat "$RUN/$1.pid")" 2>/dev/null; }

start_controller() {
  is_running controller && { echo "controller already running"; return; }
  DATA_DIR=$DATA/controller INTERNAL_TOKEN=$TOKEN \
  HEARTBEAT_TIMEOUT=${HEARTBEAT_TIMEOUT:-10} REPAIR_GRACE_PERIOD=${REPAIR_GRACE_PERIOD:-20} \
  PYTHONPATH=controller nohup "$PY" -m uvicorn --factory objstore_controller.main:create_app \
    --host 127.0.0.1 --port "$CONTROLLER_PORT" >>"$RUN/controller.log" 2>&1 &
  echo $! >"$RUN/controller.pid"
  echo "controller started on :$CONTROLLER_PORT (pid $!)"
}

start_worker() {
  local n=${1#worker} name="worker${1#worker}" port=$((9000 + ${1#worker}))
  is_running "$name" && { echo "$name already running"; return; }
  WORKER_ID=$name WORKER_URL=http://127.0.0.1:$port CONTROLLER_URL=http://127.0.0.1:$CONTROLLER_PORT \
  DATA_DIR=$DATA/$name INTERNAL_TOKEN=$TOKEN HEARTBEAT_INTERVAL=3 SCRUB_INTERVAL=${SCRUB_INTERVAL:-60} \
  PYTHONPATH=worker nohup "$PY" -m uvicorn --factory objstore_worker.main:create_app \
    --host 127.0.0.1 --port "$port" >>"$RUN/$name.log" 2>&1 &
  echo $! >"$RUN/$name.pid"
  echo "$name started on :$port (pid $!)"
}

stop_proc() {
  if is_running "$1"; then kill "$(cat "$RUN/$1.pid")" && echo "$1 stopped"; else echo "$1 not running"; fi
  rm -f "$RUN/$1.pid"
}

case "${1:-}" in
  up)
    echo "CONTROLLER_PORT=$CONTROLLER_PORT" >"$RUN/ports.env"
    start_controller
    sleep 1
    for n in 1 2 3 4; do start_worker "$n"; done ;;
  down)
    for n in 1 2 3 4; do stop_proc "worker$n"; done
    stop_proc controller ;;
  stop-worker)  stop_proc "worker${2#worker}" ;;
  start-worker) start_worker "${2:?worker number}" ;;
  status)
    for p in controller worker1 worker2 worker3 worker4; do
      if is_running "$p"; then echo "$p running"; else echo "$p stopped"; fi
    done ;;
  logs) tail -n "${3:-50}" "$RUN/${2:-controller}.log" ;;
  clean) "$0" down || true; rm -rf "$DATA"; echo "removed $DATA" ;;
  *) sed -n '2,12p' "$0"; exit 1 ;;
esac
