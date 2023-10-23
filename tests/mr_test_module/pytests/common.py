from RLTest import Env, Defaults
import json
import signal
import time
import unittest
import inspect
import os

Defaults.decode_responses = True

class TimeLimit(object):
    """
    A context manager that fires a TimeExpired exception if it does not
    return within the specified amount of time.
    """

    def __init__(self, timeout):
        self.timeout = timeout

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handler)
        signal.setitimer(signal.ITIMER_REAL, self.timeout, 0)

    def __exit__(self, exc_type, exc_value, traceback):
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)

    def handler(self, signum, frame):
        raise Exception('timeout')

class Colors(object):
    @staticmethod
    def Cyan(data):
        return '\033[36m' + data + '\033[0m'

    @staticmethod
    def Yellow(data):
        return '\033[33m' + data + '\033[0m'

    @staticmethod
    def Bold(data):
        return '\033[1m' + data + '\033[0m'

    @staticmethod
    def Bred(data):
        return '\033[31;1m' + data + '\033[0m'

    @staticmethod
    def Gray(data):
        return '\033[30;1m' + data + '\033[0m'

    @staticmethod
    def Lgray(data):
        return '\033[30;47m' + data + '\033[0m'

    @staticmethod
    def Blue(data):
        return '\033[34m' + data + '\033[0m'

    @staticmethod
    def Green(data):
        return '\033[32m' + data + '\033[0m'

BASE_JAR_FILE = './gears_tests/build/gears_tests.jar'

def getConnectionByEnv(env):
    conn = None
    if env.env == 'oss-cluster':
        conn = env.envRunner.getClusterConnection()
    else:
        conn = env.getConnection()
    return conn

def runSkipTests():
    return True if os.environ.get('RUN_SKIPED_TESTS', False) else False

def waitBeforeTestStart():
    return True if os.environ.get('HOLD', False) else False

def shardsConnections(env):
    for s in range(1, env.shardsCount + 1):
        yield env.getConnection(shardId=s)

def verifyClusterInitialized(env):
    for conn in shardsConnections(env):
        allConnected = False
        while not allConnected:
            res = conn.execute_command('MRTESTS.INFOCLUSTER')
            nodes = res[4]
            allConnected = True
            for n in nodes:
                status = n[17]
                if status != 'connected':
                    allConnected = False
            if not allConnected:
                time.sleep(0.1)

def MRTestDecorator(skipTest=False, skipOnSingleShard=False, skipOnCluster=False, skipOnValgrind=False, envArgs={}):
    def test_func_generator(test_function):
        def test_func():
            test_name = '%s:%s' % (inspect.getfile(test_function), test_function.__name__)
            if skipTest and not runSkipTests():
                raise unittest.SkipTest()
            env = Env(**envArgs)
            conn = getConnectionByEnv(env)
            if skipOnSingleShard:
                if env.shardsCount == 1:
                    raise unittest.SkipTest()
                
            if skipOnCluster:
                if 'cluster' in env.env:
                    raise unittest.SkipTest()
            if skipOnValgrind:
                if env.debugger is not None:
                    raise unittest.SkipTest()
            args = {
                'env': env,
                'conn': conn
            }
            env.broadcast('MRTESTS.REFRESHCLUSTER')
            if env.isCluster():
                # make sure cluster will not turn to failed state and we will not be
                # able to execute commands on shards, on slow envs, run with valgrind,
                # or mac, it is needed.
                env.broadcast('CONFIG', 'set', 'cluster-node-timeout', '120000')
                env.broadcast('MRTESTS.FORCESHARDSCONNECTION')
                with TimeLimit(2):
                    verifyClusterInitialized(env)
            if waitBeforeTestStart():
                input('\tpress any button to continue test %s' % test_name)
            test_function(**args)
        return test_func
    return test_func_generator
