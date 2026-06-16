import unittest

from common import MRTestDecorator, is_redis_version_is_lower_than, promote_internal_client_if_supported

@MRTestDecorator()
def testMRMapError(env, conn):
    for i in range(1000):
        conn.execute_command('set', 'key%d' % i, str(i))
    env.expect('lmrtest.maperror').equal([0, 1000])

@MRTestDecorator()
def testMRFilterError(env, conn):
    for i in range(1000):
        conn.execute_command('set', 'key%d' % i, str(i))
    env.expect('lmrtest.filtererror').equal([0, 1000])

@MRTestDecorator()
def testMRAccumulateError(env, conn):
    for i in range(1000):
        conn.execute_command('set', 'key%d' % i, str(i))
    env.expect('lmrtest.accumulatererror').equal([0, 1000])

@MRTestDecorator()
def testMRReadError(env, conn):
    env.expect('lmrtest.readerror').equal([0, env.shardsCount])

@MRTestDecorator()
def testInternalCommandFromScript(env, conn):
    env.expect('eval', "return redis.call('MRTESTS.NETWORKTEST')", '0').error().contains('command is not allowed from script')
    env.expect('eval', "return redis.call('MRTESTS.REFRESHCLUSTER')", '0').error().contains('command is not allowed from script')
    env.expect('eval', "return redis.call('MRTESTS.INNERCOMMUNICATION')", '0').error().contains('command is not allowed from script')
    env.expect('eval', "return redis.call('MRTESTS.HELLO')", '0').error().contains('command is not allowed from script')
    env.expect('eval', "return redis.call('MRTESTS.CLUSTERSET')", '0').error().contains('command is not allowed from script')
    env.expect('eval', "return redis.call('MRTESTS.CLUSTERSETFROMSHARD')", '0').error().contains('command is not allowed from script')
    env.expect('eval', "return redis.call('MRTESTS.INFOCLUSTER')", '0').error().contains('command is not allowed from script')
    env.expect('eval', "return redis.call('MRTESTS.FORCESHARDSCONNECTION')", '0').error().contains('command is not allowed from script')

@MRTestDecorator(skipClusterInitialisation=True)
def testShortFormClusterSetWithoutSlotRangesApi(env, conn):
    # The short form of CLUSTERSET relies on RedisModule_GetClusterNodeSlotRanges
    # (redis/redis#14953). On Redis builds that don't export it, the command must
    # reply with an error -- so callers (e.g. DMC) can fall back to the long form --
    # instead of silently replying OK while leaving the cluster unconfigured.
    if not is_redis_version_is_lower_than('8.0.0'):
        # Newer builds may export the API, in which case the short form is accepted.
        # That path is covered end-to-end by RedisTimeSeries' flow tests.
        raise unittest.SkipTest()
    promote_internal_client_if_supported(env=env)
    env.expect('MRTESTS.CLUSTERSET').error().contains('Failed to set cluster topology')
    # Wrong arity is rejected up front, regardless of the API's availability.
    env.expect('MRTESTS.CLUSTERSET', 'AUTH').error().contains('Could not parse cluster set arguments')
