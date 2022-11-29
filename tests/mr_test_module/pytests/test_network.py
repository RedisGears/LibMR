from common import MRTestDecorator
import gevent.server
import gevent.queue
import gevent.socket
import time
from common import TimeLimit
import socket

class Connection(object):
    def __init__(self, sock, bufsize=4096, underlying_sock=None):
        self.sock = sock
        self.sockf = sock.makefile('rwb', bufsize)
        self.closed = False
        self.peer_closed = False
        self.underlying_sock = underlying_sock

    def close(self):
        if not self.closed:
            self.closed = True
            self.sockf.close()
            self.sock.close()
            self.sockf = None

    def is_close(self, timeout=2):
        if self.closed:
            return True
        try:
            with TimeLimit(timeout):
                return self.read(1) == ''
        except Exception:
            return False

    def flush(self):
        self.sockf.flush()

    def get_address(self):
        return self.sock.getsockname()[0]

    def get_port(self):
        return self.sock.getsockname()[1]

    def read(self, bytes):
        return self.sockf.read(bytes).decode()

    def read_at_most(self, bytes, timeout=0.01):
        self.sock.settimeout(timeout)
        return self.sock.recv(bytes).decode()

    def send(self, data):
        self.sockf.write(str.encode(data))
        self.sockf.flush()

    def readline(self):
        return self.sockf.readline().decode()

    def send_bulk_header(self, data_len):
        self.sockf.write(str.encode('$%d\r\n' % data_len))
        self.sockf.flush()

    def send_bulk(self, data):
        self.sockf.write(str.encode('$%d\r\n%s\r\n' % (len(data), data)))
        self.sockf.flush()

    def send_status(self, data):
        self.sockf.write(str.encode('+%s\r\n' % data))
        self.sockf.flush()

    def send_error(self, data):
        self.sockf.write(str.encode('-%s\r\n' % data))
        self.sockf.flush()

    def send_integer(self, data):
        self.sockf.write(str.encode(':%u\r\n' % data))
        self.sockf.flush()

    def send_mbulk(self, data):
        self.sockf.write(str.encode('*%d\r\n' % len(data)))
        for elem in data:
            self.sockf.write(str.encode('$%d\r\n%s\r\n' % (len(elem), elem)))
        self.sockf.flush()

    def read_mbulk(self, args_count=None):
        if args_count is None:
            line = self.readline()
            if not line:
                self.peer_closed = True
            if not line or line[0] != '*':
                self.close()
                return None
            try:
                args_count = int(line[1:])
            except ValueError:
                raise Exception('Invalid mbulk header: %s' % line)
        data = []
        for arg in range(args_count):
            data.append(self.read_response())
        return data

    def read_request(self):
        line = self.readline()
        if not line:
            self.peer_closed = True
            self.close()
            return None
        if line[0] != '*':
            return line.rstrip().split()
        try:
            args_count = int(line[1:])
        except ValueError:
            raise Exception('Invalid mbulk request: %s' % line)
        return self.read_mbulk(args_count)

    def read_request_and_reply_status(self, status):
        req = self.read_request()
        if not req:
            return
        self.current_request = req
        self.send_status(status)

    def wait_until_writable(self, timeout=None):
        try:
            gevent.socket.wait_write(self.sockf.fileno(), timeout)
        except gevent.socket.error:
            return False
        return True

    def wait_until_readable(self, timeout=None):
        if self.closed:
            return False
        try:
            gevent.socket.wait_read(self.sockf.fileno(), timeout)
        except gevent.socket.error:
            return False
        return True

    def read_response(self):
        line = self.readline()
        if not line:
            self.peer_closed = True
            self.close()
            return None
        if line[0] == '+':
            return line.rstrip()
        elif line[0] == ':':
            try:
                return int(line[1:])
            except ValueError:
                raise Exception('Invalid numeric value: %s' % line)
        elif line[0] == '-':
            return line.rstrip()
        elif line[0] == '$':
            try:
                bulk_len = int(line[1:])
            except ValueError:
                raise Exception('Invalid bulk response: %s' % line)
            if bulk_len == -1:
                return None
            data = self.sockf.read(bulk_len + 2).decode()
            if len(data) < bulk_len:
                self.peer_closed = True
                self.close()
            return data[:bulk_len]
        elif line[0] == '*':
            try:
                args_count = int(line[1:])
            except ValueError:
                raise Exception('Invalid mbulk response: %s' % line)
            return self.read_mbulk(args_count)
        else:
            raise Exception('Invalid response: %s' % line)


class ShardMock():
    def __init__(self, env, host='localhost'):
        self.env = env
        self.new_conns = gevent.queue.Queue()
        self.host = host

    def _handle_conn(self, sock, client_addr):
        conn = Connection(sock)
        self.new_conns.put(conn)

    def _send_cluster_set(self):
        self.env.cmd('MRTESTS.CLUSTERSET',
                     'NO-USED',
                     'NO-USED',
                     'NO-USED',
                     'NO-USED',
                     'NO-USED',
                     '1',
                     'NO-USED',
                     '2',
                     'NO-USED',
                     '1',
                     'NO-USED',
                     '0',
                     '8192',
                     'NO-USED',
                     'password@%s:6379' % self.host,
                     'NO-USED',
                     'NO-USED',
                     '2',
                     'NO-USED',
                     '8193',
                     '16383',
                     'NO-USED',
                     'password@%s:10000' % self.host
                     )

    def __enter__(self):
        self.stream_server = gevent.server.StreamServer((self.host, 10000), self._handle_conn)
        self.stream_server.start()
        self._send_cluster_set()
        self.runId = self.env.cmd('MRTESTS.INFOCLUSTER')[3]
        return self

    def __exit__(self, type, value, traceback):
        self.stream_server.stop()

    def GetConnection(self, runid='1', sendHelloResponse=True):
        conn = self.new_conns.get(block=True, timeout=None)
        self.env.assertEqual(conn.read_request(), ['AUTH', 'password'])
        conn.send_status('OK')  # auth response
        if(sendHelloResponse):
            self.env.assertEqual(conn.read_request(), ['MRTESTS.HELLO'])
            conn.send_bulk(runid)  # hello response, sending runid
        conn.flush()
        return conn

    def GetCleanConnection(self):
        return self.new_conns.get(block=True, timeout=None)

    def StopListening(self):
        self.stream_server.stop()

    def StartListening(self):
        self.stream_server = gevent.server.StreamServer((self.host, 10000), self._handle_conn)
        self.stream_server.start()

def _is_ipv6_enabled():
    """Check whether IPv6 is enabled on this host."""
    if socket.has_ipv6:
        sock = None
        try:
            sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            sock.bind(("::1", 0))
            return True
        except OSError:
            pass
        finally:
            if sock:
                sock.close()
    return False

def _get_hosts():
    return ['localhost', '::0'] if _is_ipv6_enabled() else ['localhost']

@MRTestDecorator(skipOnCluster=True)
def testMessageIdCorrectness(env, conn):
    for host in _get_hosts():
        with ShardMock(env, host) as shardMock:
            conn = shardMock.GetConnection()

            env.expect('MRTESTS.NETWORKTEST').equal('OK')

            env.assertEqual(conn.read_request(), ['MRTESTS.INNERCOMMUNICATION', '0000000000000000000000000000000000000001', shardMock.runId, '0', 'test msg', '0'])
            conn.send_status('OK')

            env.expect('MRTESTS.NETWORKTEST').equal('OK')

            env.assertEqual(conn.read_request(), ['MRTESTS.INNERCOMMUNICATION', '0000000000000000000000000000000000000001', shardMock.runId, '0', 'test msg', '1'])
            conn.send_status('OK')

@MRTestDecorator(skipOnCluster=True)
def testErrorHelloResponse(env, conn):
    for host in _get_hosts():
        with ShardMock(env, host) as shardMock:
            conn = shardMock.GetCleanConnection()
            env.assertEqual(conn.read_request(), ['AUTH', 'password'])
            env.assertEqual(conn.read_request(), ['MRTESTS.HELLO'])
            conn.send_status('OK')  # auth response
            conn.send_error('err')  # sending error for the RG.HELLO request

            # expect the rg.hello to be sent again
            env.assertEqual(conn.read_request(), ['MRTESTS.HELLO'])

            # closing the connection befor reply
            conn.close()

            # expect a new connection to arrive
            conn = shardMock.GetConnection()

@MRTestDecorator(skipOnCluster=True)
def testClusterErrorHelloResponse(env, conn):
    for host in _get_hosts():
        with ShardMock(env, host) as shardMock:
            conn = shardMock.GetCleanConnection()
            env.assertEqual(conn.read_request(), ['AUTH', 'password'])
            env.assertEqual(conn.read_request(), ['MRTESTS.HELLO'])
            conn.send_status('OK')  # auth response
            conn.send_error('ERRCLUSTER')  # sending error for the RG.HELLO request

            # expect the topology rg.hello to be sent
            env.assertEqual(conn.read_request(), ['MRTESTS.CLUSTERSETFROMSHARD', 'NO-USED', 'NO-USED', 'NO-USED', 'NO-USED', 'NO-USED', '0000000000000000000000000000000000000002', 'NO-USED', '2', 'NO-USED', '1', 'NO-USED', '0', '8192', 'NO-USED', 'password@%s:6379' % shardMock.host, 'NO-USED', 'NO-USED', '2', 'NO-USED', '8193', '16383', 'NO-USED', 'password@%s:10000' % shardMock.host])
            env.assertEqual(conn.read_request(), ['MRTESTS.HELLO'])

            # closing the connection befor reply
            conn.close()

            # expect a new connection to arrive
            conn = shardMock.GetConnection()

@MRTestDecorator(skipOnCluster=True)
def testMessageResentAfterDisconnect(env, conn):
    for host in _get_hosts():
        with ShardMock(env, host) as shardMock:
            conn = shardMock.GetConnection()

            env.expect('MRTESTS.NETWORKTEST').equal('OK')

            env.assertEqual(conn.read_request(), ['MRTESTS.INNERCOMMUNICATION', '0000000000000000000000000000000000000001', shardMock.runId, '0', 'test msg', '0'])

            conn.send_status('OK')

            env.expect('MRTESTS.NETWORKTEST').equal('OK')

            env.assertEqual(conn.read_request(), ['MRTESTS.INNERCOMMUNICATION', '0000000000000000000000000000000000000001', shardMock.runId, '0', 'test msg', '1'])

            conn.close()

            conn = shardMock.GetConnection()

            env.assertEqual(conn.read_request(), ['MRTESTS.INNERCOMMUNICATION', '0000000000000000000000000000000000000001', shardMock.runId, '0', 'test msg', '1'])

            conn.send_status('duplicate message ignored')  # reply to the second message with duplicate reply

            conn.close()

            conn = shardMock.GetConnection()

            # make sure message 2 will not be sent again
            try:
                with TimeLimit(1):
                    conn.read_request()
                    env.assertTrue(False)  # we should not get any data after crash
            except Exception:
                pass

@MRTestDecorator(skipOnCluster=True)
def testMessageNotResentAfterCrash(env, conn):
    for host in _get_hosts():
        with ShardMock(env, host) as shardMock:
            conn = shardMock.GetConnection()

            env.expect('MRTESTS.NETWORKTEST').equal('OK')

            env.assertEqual(conn.read_request(), ['MRTESTS.INNERCOMMUNICATION', '0000000000000000000000000000000000000001', shardMock.runId, '0', 'test msg', '0'])

            conn.send_status('OK')

            env.expect('MRTESTS.NETWORKTEST').equal('OK')

            env.assertEqual(conn.read_request(), ['MRTESTS.INNERCOMMUNICATION', '0000000000000000000000000000000000000001', shardMock.runId, '0', 'test msg', '1'])

            conn.close()

            conn = shardMock.GetConnection(runid='2')  # shard crash

            try:
                with TimeLimit(1):
                    conn.read_request()
                    env.assertTrue(False)  # we should not get any data after crash
            except Exception:
                pass

@MRTestDecorator(skipOnCluster=True)
def testSendRetriesMechanizm(env, conn):
    for host in _get_hosts():
        with ShardMock(env, host) as shardMock:
            conn = shardMock.GetConnection()

            env.expect('MRTESTS.NETWORKTEST').equal('OK')

            env.assertEqual(conn.read_request(), ['MRTESTS.INNERCOMMUNICATION', '0000000000000000000000000000000000000001', shardMock.runId, '0', 'test msg', '0'])

            conn.send('-Err\r\n')

            env.assertTrue(conn.is_close())

            # should be a retry

            conn = shardMock.GetConnection()

            env.assertEqual(conn.read_request(), ['MRTESTS.INNERCOMMUNICATION', '0000000000000000000000000000000000000001', shardMock.runId, '0', 'test msg', '0'])

            conn.send('-Err\r\n')

            env.assertTrue(conn.is_close())

            # should be a retry

            conn = shardMock.GetConnection()

            env.assertEqual(conn.read_request(), ['MRTESTS.INNERCOMMUNICATION', '0000000000000000000000000000000000000001', shardMock.runId, '0', 'test msg', '0'])

            conn.send('-Err\r\n')

            env.assertTrue(conn.is_close())

            # should not retry

            conn = shardMock.GetConnection()

            # make sure message will not be sent again
            try:
                with TimeLimit(1):
                    conn.read_request()
                    env.assertTrue(False)  # we should not get any data after crash
            except Exception:
                pass

@MRTestDecorator(skipOnCluster=True)
def testSendTopology(env, conn):
    for host in _get_hosts():
        with ShardMock(env, host) as shardMock:
            conn = shardMock.GetConnection()

            env.expect('MRTESTS.NETWORKTEST').equal('OK')

            env.assertEqual(conn.read_request(), ['MRTESTS.INNERCOMMUNICATION', '0000000000000000000000000000000000000001', shardMock.runId, '0', 'test msg', '0'])

            conn.send_error('ERRCLUSTER')

            env.assertTrue(conn.is_close())

            # should reconnect
            conn = shardMock.GetConnection(sendHelloResponse=False)

            # should recieve the topology
            env.assertEqual(conn.read_request(), ['MRTESTS.CLUSTERSETFROMSHARD', 'NO-USED', 'NO-USED', 'NO-USED', 'NO-USED', 'NO-USED', '0000000000000000000000000000000000000002', 'NO-USED', '2', 'NO-USED', '1', 'NO-USED', '0', '8192', 'NO-USED', 'password@%s:6379' % shardMock.host, 'NO-USED', 'NO-USED', '2', 'NO-USED', '8193', '16383', 'NO-USED', 'password@%s:10000' % shardMock.host])

@MRTestDecorator(skipOnCluster=True)
def testStopListening(env, conn):
    for host in _get_hosts():
        with ShardMock(env, host) as shardMock:
            conn = shardMock.GetConnection()

            env.expect('MRTESTS.NETWORKTEST').equal('OK')

            env.assertEqual(conn.read_request(), ['MRTESTS.INNERCOMMUNICATION', '0000000000000000000000000000000000000001', shardMock.runId, '0', 'test msg', '0'])

            conn.send_status('OK')

            shardMock.StopListening()

            conn.close()

            time.sleep(0.5)

            env.expect('MRTESTS.NETWORKTEST').equal('OK')

            shardMock.StartListening()

            conn = shardMock.GetConnection()

            env.assertEqual(conn.read_request(), ['MRTESTS.INNERCOMMUNICATION', '0000000000000000000000000000000000000001', shardMock.runId, '0', 'test msg', '1'])

@MRTestDecorator(skipOnCluster=True)
def testDuplicateMessagesAreIgnored(env, conn):
    for host in _get_hosts():
        with ShardMock(env, host) as shardMock:
            shardMock.GetConnection()
            env.expect('MRTESTS.INNERCOMMUNICATION', '0000000000000000000000000000000000000002', '0000000000000000000000000000000000000000' , '0', 'test msg', '0').equal('OK')
            env.expect('MRTESTS.INNERCOMMUNICATION', '0000000000000000000000000000000000000002', '0000000000000000000000000000000000000000' , '0', 'test msg', '0').equal('duplicate message ignored')

@MRTestDecorator(skipOnCluster=True)
def testMessagesResentAfterHelloResponse(env, conn):
    for host in _get_hosts():
        with ShardMock(env, host) as shardMock:
            conn = shardMock.GetConnection(sendHelloResponse=False)

            # read RG.HELLO request
            env.assertEqual(conn.read_request(), ['MRTESTS.HELLO'])

            # this will send 'test' msg to the shard before he replied the RG.HELLO message
            env.expect('MRTESTS.NETWORKTEST').equal('OK')

            # send RG.HELLO reply
            conn.send_bulk('1')  # hello response, sending runid

            # make sure we get the 'test' msg
            try:
                with TimeLimit(2):
                    env.assertEqual(conn.read_request(), ['MRTESTS.INNERCOMMUNICATION', '0000000000000000000000000000000000000001', shardMock.runId, '0', 'test msg', '0'])
            except Exception:
                env.assertTrue(False, message='did not get the "test" message')

@MRTestDecorator(skipOnSingleShard=True)
def testClusterRefreshOnOnlySingleNode(env, conn):
    env.expect('lmrtest.readerror').equal([0, env.shardsCount])
    env.cmd('MRTESTS.REFRESHCLUSTER')
    try:
        with TimeLimit(2):
            res = env.cmd('lmrtest.readerror')
            env.assertEqual(res, [0, env.shardsCount])
    except Exception as e:
        env.assertTrue(False, message='Failed waiting for execution to finish')

@MRTestDecorator(skipOnCluster=True)
def testClusterSetAfterHelloResponseFailure(env, conn):
    for host in _get_hosts():
        with ShardMock(env, host) as shardMock:
            conn = shardMock.GetConnection(sendHelloResponse=False)

            # read RG.HELLO request
            env.assertEqual(conn.read_request(), ['MRTESTS.HELLO'])

            # send RG.HELLO bad reply
            conn.send_error('err')  # hello response, sending runid

            # resend cluster set
            res = env.cmd('MRTESTS.CLUSTERSET',
                        'NO-USED',
                        'NO-USED',
                        'NO-USED',
                        'NO-USED',
                        'NO-USED',
                        '1',
                        'NO-USED',
                        '1',
                        'NO-USED',
                        '1',
                        'NO-USED',
                        '0',
                        '8192',
                        'NO-USED',
                        'password@%s:6379' % shardMock.host,
                        'NO-USED',
                        )

            time.sleep(2) # make sure the RG.HELLO resend callback is not called

@MRTestDecorator(skipOnCluster=True)
def testClusterSetAfterDisconnect(env, conn):
    for host in _get_hosts():
        with ShardMock(env, host) as shardMock:
            conn = shardMock.GetConnection(sendHelloResponse=False)

            # read RG.HELLO request
            env.assertEqual(conn.read_request(), ['MRTESTS.HELLO'])

            conn.close()

            # resend cluster set
            res = env.cmd('MRTESTS.CLUSTERSET',
                        'NO-USED',
                        'NO-USED',
                        'NO-USED',
                        'NO-USED',
                        'NO-USED',
                        '1',
                        'NO-USED',
                        '1',
                        'NO-USED',
                        '1',
                        'NO-USED',
                        '0',
                        '8192',
                        'NO-USED',
                        'password@%s:6379' % shardMock.host,
                        'NO-USED',
                        )

            shardMock._send_cluster_set()

            conn = shardMock.GetConnection(sendHelloResponse=False)

            # read RG.HELLO request
            env.assertEqual(conn.read_request(), ['MRTESTS.HELLO'])

@MRTestDecorator(skipOnCluster=True)
def testMassiveClusterSet(env, conn):
    for host in _get_hosts():
        with ShardMock(env, host) as shardMock:
            for i in range(1000):
                conn = shardMock.GetConnection(sendHelloResponse=False)
                shardMock._send_cluster_set()

@MRTestDecorator(skipOnCluster=True)
def testMassiveClusterSetFromShard(env, conn):
    for host in _get_hosts():
        with ShardMock(env, host) as shardMock:
            for i in range(1000):
                env.cmd('MRTESTS.CLUSTERSETFROMSHARD',
                        'NO-USED',
                        'NO-USED',
                        'NO-USED',
                        'NO-USED',
                        'NO-USED',
                        '1',
                        'NO-USED',
                        '2',
                        'NO-USED',
                        '1',
                        'NO-USED',
                        '0',
                        '8192',
                        'NO-USED',
                        'password@%s:6379' % shardMock.host,
                        'NO-USED',
                        'NO-USED',
                        '2',
                        'NO-USED',
                        '8193',
                        '16383',
                        'NO-USED',
                        'password@%s:10000' % shardMock.host
                        )