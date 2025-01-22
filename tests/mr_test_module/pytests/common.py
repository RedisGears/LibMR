from RLTest import Env, Defaults
import json
import signal
import time
import unittest
import inspect
import os
import tempfile
from packaging import version

Defaults.decode_responses = True


class ShardsConnectionTimeoutException(Exception):
    pass

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
        raise ShardsConnectionTimeoutException()

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
        try:
            # try to promote to internal connection
            conn.execute_command('debug', 'PROMOTE-CONN')
        except Exception:
            pass
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

def initialiseCluster(env):
    env.broadcast('MRTESTS.REFRESHCLUSTER')
    if env.isCluster():
        # make sure cluster will not turn to failed state and we will not be
        # able to execute commands on shards, on slow envs, run with valgrind,
        # or mac, it is needed.
        env.broadcast('CONFIG', 'set', 'cluster-node-timeout', '120000')
        for conn in shardsConnections(env):
            try:
                conn.execute_command('debug', 'PROMOTE-CONN')
            except Exception:
                pass
            conn.execute_command('MRTESTS.FORCESHARDSCONNECTION')
        with TimeLimit(2):
            verifyClusterInitialized(env)

# Creates a temporary file with the content provided.
# Returns the filepath of the created file.
def create_config_file(content) -> str:
    with tempfile.NamedTemporaryFile(delete=False, dir=os.getcwd()) as f:
        f.write(content.encode())
        return f.name

# Returns the redis-server version without starting the server.
def get_redis_version():
    redis_binary = os.environ.get('REDIS_SERVER', Defaults.binary)
    version_output = os.popen('%s --version' % redis_binary).read()
    version_number = version_output.split()[2][2:].strip()
    return version.parse(version_number)

def is_redis_version_is_lower_than(required_version):
    return get_redis_version() < version.parse(required_version)

def skip_if_redis_version_is_lower_than(required_version):
    if is_redis_version_is_lower_than(required_version):
        raise unittest.SkipTest()

def MRTestDecorator(redisConfigFileContent=None, moduleArgs=None, skipTest=False, skipClusterInitialisation=False, skipOnVersionLowerThan=None, skipOnSingleShard=False, skipOnCluster=False, skipOnValgrind=False, envArgs={}):
    def test_func_generator(test_function):
        def test_func():
            test_name = '%s:%s' % (inspect.getfile(test_function), test_function.__name__)
            if skipTest and not runSkipTests():
                raise unittest.SkipTest()
            if skipOnVersionLowerThan:
                skip_if_redis_version_is_lower_than(skipOnVersionLowerThan)
            defaultModuleArgs = 'password'
            if not is_redis_version_is_lower_than('8.0.0'):
                # We provide password only if version < 8.0.0. If version is greater, we have internal command and we do not need the password.
                defaultModuleArgs = None
            envArgs['moduleArgs'] = moduleArgs or defaultModuleArgs
            envArgs['redisConfigFile'] = create_config_file(redisConfigFileContent) if redisConfigFileContent else None
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
            if not skipClusterInitialisation:
                initialiseCluster(env)
            if waitBeforeTestStart():
                input('\tpress any button to continue test %s' % test_name)
            test_function(**args)
        return test_func
    return test_func_generator
