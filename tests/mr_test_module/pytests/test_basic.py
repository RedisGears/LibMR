from common import MRTestDecorator

@MRTestDecorator()
def testBasicMR(env, conn):
    for i in range(1000):
        conn.execute_command('set', 'key%d' % i, str(i))
    res = env.cmd('lmrtest.readallkeys')
    env.assertEqual(sorted(res), sorted(['key%d' % i for i in range(1000)]))

@MRTestDecorator()
def testBasicMRMap(env, conn):
    for i in range(1000):
        conn.execute_command('set', 'key%d' % i, str(i))
    res = env.cmd('lmrtest.readallkeystype')
    env.assertEqual(sorted(res), sorted(['string' for i in range(1000)]))

@MRTestDecorator()
def testBasicMRFilter(env, conn):
    for i in range(1000):
        conn.execute_command('set', 'key%d' % i, str(i))
    for i in range(1000):
        conn.execute_command('hset', 'doc%d' % i, 'foo', 'bar')
    res = env.cmd('lmrtest.readallstringkeys')
    env.assertEqual(sorted(res), sorted(['key%d' % i for i in range(1000)]))

@MRTestDecorator()
def testBasicMRReshuffle(env, conn):
    for i in range(1000):
        conn.execute_command('set', 'key%d' % i, str(i))
    res = env.cmd('lmrtest.replacekeysvalues', 'key')
    env.assertEqual(sorted(res), sorted(['OK' for i in range(1000)]))
    for i in range(1000):
        env.assertTrue(conn.execute_command('exists', str(i)))

@MRTestDecorator()
def testBasicMRAccumulate(env, conn):
    for i in range(1000):
        conn.execute_command('set', 'key%d' % i, str(i))
    env.expect('lmrtest.countkeys').equal([1000])

@MRTestDecorator()
def testBasicMRMassiveData(env, conn):
    for i in range(100000):
        conn.execute_command('set', 'key%d' % i, str(i))
    env.expect('lmrtest.countkeys').equal([100000])

@MRTestDecorator(skipOnSingleShard=True)
def testMaxIdle(env, conn):
    env.expect('lmrtest.reachmaxidle').error().contains('execution max idle reached')
