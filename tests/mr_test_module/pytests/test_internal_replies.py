from concurrent.futures import ThreadPoolExecutor

from common import MRTestDecorator


@MRTestDecorator(skipOnSingleShard=True, skipOnVersionLowerThan='8.0.0')
def testInternalReplyWorkerOptIn(env, conn):
    count = env.shardsCount * 2

    def check(shard):
        connection = env.getConnection(shard)
        try:
            for _ in range(20):
                # The legacy parser runs on the event loop, distinct from on-done.
                assert connection.execute_command('lmrtest.internalreply') == [count, 0, 0]
                # Opted-in parsing and on-done run in the same worker task.
                assert connection.execute_command('lmrtest.internalreply', 'WORKERS') == [count, 0, count]
                # Preparation/batch lifecycle assert main-thread ownership in C/Rust;
                # execution asserts that it has moved to a different thread.
                assert connection.execute_command('lmrtest.internalreply', 'PREPARED') == [count, 0, count]
                # Ordinary step data matching the hint marker is not metadata.
                assert connection.execute_command('lmrtest.internalreply', 'HINTSTRING') == [count, 0, count]
        finally:
            connection.close()

    with ThreadPoolExecutor(max_workers=env.shardsCount * 3) as pool:
        list(pool.map(check, list(range(1, env.shardsCount + 1)) * 3))
