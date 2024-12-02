import pytest
from common import MRTestDecorator, initialiseCluster, TimeLimit
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

@MRTestDecorator(moduleArgs='default')
def testAclSetting(env, conn):
    '''
    Tests that LibMR sets the ACLs for its commands.
    '''
    env.skipOnVersionSmaller('7.4.0')
    acl_category = '_MRTESTS_libmr_internal'
    env.expect('acl', 'cat').contains(acl_category)

    # Test that the user not allowed to run the commands, as this
    # module uses the "default" user to run the commands instead.
    command = 'lmrtest.dbsize'
    env.expect('ACL', 'SETUSER', 'user1', 'on', '>user1p', '-@all', '+%s' % command).contains('OK')
    env.expect('ACL', 'SETUSER', 'user2', 'on', '>user2p', '+@all', '-@%s' % acl_category).contains('OK')
    env.expect('AUTH', 'user1', 'user1p').equal(True)

    env.expect('lmrtest.dbsize').equal(0)

    # This should succeed even though the user is not allowed to run
    # the commands of libmr. This is so, because the module itself runs
    # the LibMR commands as the other user specified during the load,
    # which has the necessary permissions.
    env.expect('AUTH', 'user2', 'user2p').equal(True)
    env.expect('lmrtest.dbsize').equal(0)

@MRTestDecorator(
    commandsBeforeClusterStart=['ACL SETUSER baduser on >password -@_MRTESTS_libmr_internal'],
    moduleArgs='baduser',
    skipShardInitialisation=True,
    skipOnVersionLowerThan='7.4.0',
)
def testAclSettingNotWorksWhenItShouldnt(env, conn):
    '''
    Tests that LibMR doesn't work when the user provided for it doesn't
    have the necessary permissions to run the LibMR commands.
    '''

    # This should fail as the LibMR will attempt to connect to the
    # shards using the "baduser" user, which doesn't have the necessary
    # permissions to run the LibMR commands.
    if env.isCluster():
        with pytest.raises(Exception):
            initialiseCluster(env)
