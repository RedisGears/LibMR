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

