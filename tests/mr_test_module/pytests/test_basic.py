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
