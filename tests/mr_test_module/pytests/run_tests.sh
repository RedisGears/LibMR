#!/usr/bin/env bash

[[ $VERBOSE == 1 ]] && set -x
[[ $IGNERR == 1 ]] || set -e

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(cd $HERE/../../../ && pwd)
OS=$(uname -s 2>/dev/null)
if [[ $OS == Darwin ]]; then
    LIB_EXTENTION=dylib
else
    LIB_EXTENTION=so
fi

if [[ $DEBUG == 1 ]]; then
    MODULE_PATH=$HERE/../target/debug/libmr_test.$LIB_EXTENTION
else
    MODULE_PATH=$HERE/../target/release/libmr_test.$LIB_EXTENTION
fi

export PYTHONUNBUFFERED=1

check_python_deps() {
    # RLTest uses redis-py 'protocol=' kwarg (redis>=5). Fail fast with a clear hint.
    "${PYTHON:-python}" - <<'PY'
import inspect
import sys
try:
  import redis
except Exception as e:
  sys.stderr.write(f"ERROR: failed to import redis-py: {e}\n")
  sys.exit(2)

has_protocol = 'protocol' in inspect.signature(redis.Redis.__init__).parameters
if not has_protocol:
  sys.stderr.write(
    f"ERROR: redis-py is too old ({getattr(redis, '__version__', 'unknown')}); "
    "RLTest requires redis>=5 for the 'protocol' kwarg.\n"
    "Fix: pip install -r tests/mr_test_module/pytests/requirements.txt\n"
  )
  sys.exit(2)
PY
}

run_with_timeout() {
    local timeout_sec="$1"
    shift

    if [[ -z $timeout_sec || $timeout_sec == 0 ]]; then
        "$@"
        return $?
    fi

    # Prefer coreutils 'timeout' when available (Linux CI).
    if command -v timeout >/dev/null 2>&1; then
        timeout --signal=KILL "${timeout_sec}s" "$@"
        return $?
    fi
    # macOS users may have coreutils as 'gtimeout'.
    if command -v gtimeout >/dev/null 2>&1; then
        gtimeout --signal=KILL "${timeout_sec}s" "$@"
        return $?
    fi

    # Portable fallback: Python subprocess timeout.
    "${PYTHON:-python}" - "$timeout_sec" "$@" <<'PY'
import subprocess, sys
timeout = int(sys.argv[1])
cmd = sys.argv[2:]
try:
  p = subprocess.run(cmd, timeout=timeout)
  sys.exit(p.returncode)
except subprocess.TimeoutExpired:
  sys.stderr.write(f"RLTest run timed out after {timeout} seconds: {' '.join(cmd)}\n")
  sys.exit(124)
PY
}

RLTEST_ARGS=(
    --verbose-information-on-failure
    --no-progress
    --randomize-ports
    --module "$MODULE_PATH"
    --clear-logs
    --oss_password "password"
    --enable-debug-command
)

# In GitHub Actions, avoid "silent forever" hangs by setting a default hard
# timeout unless explicitly overridden.
if [[ -n $GITHUB_ACTIONS && ( -z ${RUN_TIMEOUT_SEC+x} || $RUN_TIMEOUT_SEC == 0 ) ]]; then
    RUN_TIMEOUT_SEC=1800
fi

# Optional: RLTest per-test timeout (seconds)
if [[ -n $TEST_TIMEOUT_SEC && $TEST_TIMEOUT_SEC != 0 ]]; then
    RLTEST_ARGS+=(--test-timeout "$TEST_TIMEOUT_SEC")
elif [[ -n $GITHUB_ACTIONS && ( -z ${TEST_TIMEOUT_SEC+x} || $TEST_TIMEOUT_SEC == 0 ) ]]; then
    TEST_TIMEOUT_SEC=120
    RLTEST_ARGS+=(--test-timeout "$TEST_TIMEOUT_SEC")
fi

# Optional: hard timeout for the entire RLTest invocation (seconds)
RUN_TIMEOUT_SEC=${RUN_TIMEOUT_SEC:-0}

check_python_deps

set +e
# Heartbeat so CI logs keep moving even if RLTest blocks during env startup.
if [[ -n $GITHUB_ACTIONS && $RUN_TIMEOUT_SEC != 0 ]]; then
    (
        i=0
        while true; do
            sleep 60
            i=$((i + 60))
            echo "[heartbeat] RLTest still running (${i}s elapsed, RUN_TIMEOUT_SEC=${RUN_TIMEOUT_SEC})"
        done
    ) &
    HB_PID=$!
fi

run_with_timeout "$RUN_TIMEOUT_SEC" "${PYTHON:-python}" -m RLTest "${RLTEST_ARGS[@]}" "$@"
E=$?
set -e

if [[ -n ${HB_PID:-} ]]; then
    kill "$HB_PID" >/dev/null 2>&1 || true
fi

if [[ $E == 124 ]]; then
    echo "RLTest timed out (RUN_TIMEOUT_SEC=$RUN_TIMEOUT_SEC). Diagnostics:"
    echo "== ps (redis-server/RLTest) =="
    ps aux | egrep 'redis-server|RLTest' | egrep -v egrep || true
    echo "== logs =="
    ls -la "$HERE/logs" 2>/dev/null || true
    find "$HERE/logs" -type f -name "*.log" 2>/dev/null | tail -n 10 | while read f; do
        echo "--- tail $f"
        tail -n 120 "$f" || true
    done
fi

exit $E
