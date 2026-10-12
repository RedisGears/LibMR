"""Run with the response_progress C fixture and an RLTest cluster of >=3 shards."""
import concurrent.futures
import time
import subprocess
import tempfile
from RLTest import Env, Defaults


def test_response_progress():
    config = 'bind 127.0.0.1\n'
    if not Defaults.use_TLS:
        # New Redis builds require explicit acknowledgement for a plaintext
        # cluster bus. Probe without opening a port; older Redis rejects the
        # unknown directive. All fixture shards bind to loopback only.
        probe = subprocess.run(
            [Defaults.binary, '--port', '0', '--save', '',
             '--cluster-bus-port-protected-mode', 'no'],
            capture_output=True, text=True, timeout=5)
        if 'Bad directive' not in probe.stdout + probe.stderr:
            config += 'cluster-bus-port-protected-mode no\n'
    with tempfile.NamedTemporaryFile(mode='w', suffix='.conf') as config_file:
        config_file.write(config)
        config_file.flush()
        env = Env(decodeResponses=True, redisConfigFile=config_file.name)
    assert env.shardsCount >= 3
    nodes = [env.getConnection(i) for i in range(1, env.shardsCount + 1)]
    for node in nodes:
        node.execute_command('progress.refresh')
    conn = nodes[0]
    # A successful round trip also verifies topology and connections are ready.
    size = (env.shardsCount - 1) * 128 * 1024
    assert conn.execute_command('progress.query', 0, 0, 1, 1000, 5000) == size

    def query(first, gap, count, progress, deadline, *extra):
        start = time.monotonic()
        try:
            result = conn.execute_command('progress.query', first, gap, count, progress, deadline, *extra)
            return time.monotonic() - start, result
        except Exception as exc:
            return time.monotonic() - start, str(exc)

    # Large records are reassembled intact while total duration exceeds idle limit.
    elapsed, result = query(0, 100, 12, 500, 4000)
    assert result == size * 12, (elapsed, result)
    assert elapsed > 1.0
    print('PROGRESS_FIXTURE continuing', elapsed, result, flush=True)

    # Computing before first byte is permitted up to the independent deadline.
    elapsed, result = query(700, 0, 1, 200, 3000)
    assert result == size and elapsed >= .6, (elapsed, result)
    print('PROGRESS_FIXTURE first_byte', elapsed, result, flush=True)

    # A fast shard must not keep a stalled shard alive.
    stalled = nodes[1].execute_command('CLUSTER', 'MYID')
    elapsed, result = query(0, 1800, 2, 300, 4000, stalled)
    assert 'response progress timeout' in str(result), (elapsed, result)
    assert .25 <= elapsed < 1.2, elapsed
    print('PROGRESS_FIXTURE peer_isolation', elapsed, result, flush=True)
    time.sleep(2)

    # Independent executions share the same connection, but not their timers.
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        stuck = pool.submit(query, 0, 1800, 2, 300, 4000)
        active = pool.submit(query, 0, 100, 14, 500, 4000)
        elapsed, result = stuck.result(timeout=6)
        assert 'response progress timeout' in str(result), (elapsed, result)
        assert elapsed < 1.2, elapsed
        elapsed2, result2 = active.result(timeout=6)
        assert result2 == size * 14, (elapsed2, result2)
        print('PROGRESS_FIXTURE execution_isolation', elapsed, elapsed2, flush=True)
    time.sleep(2)

    # Continued progress cannot reset the hard deadline.
    elapsed, result = query(0, 100, 15, 500, 700)
    assert 'response deadline reached' in str(result), (elapsed, result)
    assert .6 <= elapsed < 1.3, elapsed
    print('PROGRESS_FIXTURE hard_deadline', elapsed, result, flush=True)
    time.sleep(2)

    elapsed, result = query(1500, 0, 1, 200, 600)
    assert 'response deadline reached' in str(result), (elapsed, result)
    assert .5 <= elapsed < 1.2, elapsed
    print('PROGRESS_FIXTURE no_first_byte', elapsed, result, flush=True)
    time.sleep(2)
    # Late frames after cancellation must not corrupt the next execution.
    assert conn.execute_command('progress.query', 0, 0, 1, 1000, 5000) == size
    for args in ((0, 0, 1, 0, 1000), (0, 0, 1, 1000, 100)):
        elapsed, result = query(*args)
        assert 'invalid response timeout policy' in str(result)
