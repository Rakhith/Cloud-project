#!/usr/bin/env sh
# Run the automated test suite in an isolated virtual environment.
#
# Usage:
#   scripts/test.sh                 # all tests
#   scripts/test.sh api             # REST API and metadata tests
#   scripts/test.sh failures        # worker-stop/restart and repair tests
#   scripts/test.sh integrity       # corruption detection and restoration
#   scripts/test.sh worker          # on-disk worker storage tests
#   scripts/test.sh cli             # command-line client tests
#
# Set VENV_DIR to keep the environment outside this repository, for example:
#   VENV_DIR=/tmp/objstore-venv scripts/test.sh integrity
# Set UPGRADE_PIP=1 only when you explicitly want to update pip.

set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
VENV_DIR=${VENV_DIR:-"$ROOT/.venv"}
PYTHON_BIN=${PYTHON_BIN:-}

case "${1:-all}" in
  all)       TEST_TARGET="tests" ;;
  api)       TEST_TARGET="tests/test_object_api.py" ;;
  failures)  TEST_TARGET="tests/test_failures.py" ;;
  integrity) TEST_TARGET="tests/test_integrity.py" ;;
  worker)    TEST_TARGET="tests/test_worker_storage.py" ;;
  cli)       TEST_TARGET="tests/test_cli.py" ;;
  *)
    echo "Usage: $0 [all|api|failures|integrity|worker|cli]" >&2
    exit 2
    ;;
esac

if [ -z "$PYTHON_BIN" ]; then
  # Match the Docker images.  Prefer a supported interpreter instead of a
  # system `python3` that may point to a newer, unsupported development build.
  for candidate in python3.13 python3.12 python3.11 python3.10 python3; do
    if command -v "$candidate" >/dev/null 2>&1; then
      PYTHON_BIN=$candidate
      break
    fi
  done
fi

if [ -z "$PYTHON_BIN" ]; then
  echo "Python 3.10, 3.11, 3.12, or 3.13 is required to create the test environment." >&2
  exit 1
fi

if [ ! -x "$VENV_DIR/bin/python" ]; then
  echo "Creating isolated test environment at $VENV_DIR"
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

if [ "${UPGRADE_PIP:-0}" = "1" ]; then
  "$VENV_DIR/bin/python" -m pip install --upgrade pip
fi
"$VENV_DIR/bin/python" -m pip install -r "$ROOT/requirements-dev.txt"
exec "$VENV_DIR/bin/python" -m pytest "$ROOT/$TEST_TARGET"
