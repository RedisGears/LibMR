from common import MRTestDecorator

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
