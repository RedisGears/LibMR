from common import MRTestDecorator
from common import TimeLimit
import time

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

@MRTestDecorator(skipOnValgrind=True)
def testBasicMRMassiveData(env, conn):
    for i in range(100000):
        conn.execute_command('set', 'key%d' % i, str(i))
    env.expect('lmrtest.countkeys').equal([100000])

@MRTestDecorator(skipOnSingleShard=True)
def testMaxIdle(env, conn):
    env.expect('lmrtest.reachmaxidle').error().contains('execution max idle reached')

@MRTestDecorator()
def testUnevenWork(env, conn):
    env.expect('lmrtest.unevenwork').equal(['record'])
    try:
        with TimeLimit(2):
            while True:
                for i in range(1, env.shardsCount + 1):
                    c = env.getConnection(i)
                    env.assertTrue(c.ping())
                time.sleep(0.1)
    except Exception as e:
        if str(e) != 'timeout':
            raise e

@MRTestDecorator()
def testRemoteTaskOnKey(env, conn):
    conn.execute_command('set', 'x', '1')
    env.expect('lmrtest.get', 'x').equal('1')
    env.expect('lmrtest.get', 'y').error().contains('bad result returned from')

@MRTestDecorator()
def testRemoteTaskOnAllShards(env, conn):
    for i in range(100):
        conn.execute_command('set', 'doc%d' % i, '1')
    env.expect('lmrtest.dbsize').equal(100)
    for i in range(100):
        conn.execute_command('del', 'doc%d' % i)
    env.expect('lmrtest.dbsize').equal(0)

@MRTestDecorator()
def testAclSetting(env, conn):
    result = conn.execute_command('acl', 'cat')
    env.expect(result).contains('lmrtest.get')

