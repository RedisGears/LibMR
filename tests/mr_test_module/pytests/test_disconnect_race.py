"""
Race summary inside the libmr event-loop thread:
  1. MR_OnConnectCallback is fired by hiredis once the TCP connect completes.
     For a TLS-enabled cluster it then calls redisInitiateSSL on the brand-new
     redisAsyncContext.  When the peer is plain TCP (or otherwise terminates
     the handshake abruptly), redisInitiateSSL returns REDIS_ERR.
     MR_OnConnectCallback logs "SSL auth ... failed", schedules a deferred
     task via MR_EventLoopAddTask(MR_ClusterAsyncDisconnect, n) and returns.
     The deferred task fires on the next event-loop iteration.
  2. Hiredis sees the now-broken context and immediately drives its own
     disconnect path which calls back into MR_ClusterOnDisconnectCallback;
     that callback sets `n->c = NULL` (and hiredis frees the context shortly
     after).
  3. The deferred MR_ClusterAsyncDisconnect task runs and executes
     redisAsyncFree(n->c).  Because n->c is now NULL, the very first
     instruction of redisAsyncFree dereferences offset 0x90 of a NULL
     pointer and the process is killed by SIGSEGV.

Reproduction strategy:
  - Run under a TLS-enabled Redis (--tls).  This forces MR_OnConnectCallback
    down the SSL branch.
  - Stand up a mock peer that accepts TCP but does NOT speak TLS.
    SSL_connect from libmr will then fail, scheduling the deferred
    MR_ClusterAsyncDisconnect.
  - Force a connection attempt and wait long enough for the deferred task
    to run.  Repeat several times to make the test robust under loaded
    machines.
  - Assert the Redis process is still alive after each cycle.
"""

import gevent.server
import gevent.queue
import socket
import time
import unittest

from common import MRTestDecorator, promote_internal_client_if_supported
from test_network import _get_hosts


class _RejectingShard(object):
    """Mock shard that accepts TCP and immediately closes the socket so any
    TLS handshake from libmr fails with an EOF / handshake error."""

    def __init__(self, host):
        self.host = host
        self.connections_accepted = 0
        self.queue = gevent.queue.Queue()

    def _handle_conn(self, sock, _addr):
        self.connections_accepted += 1
        self.queue.put(True)
        try:
            sock.close()
        except Exception:
            pass

    def __enter__(self):
        family = socket.AF_INET6 if ':' in self.host else socket.AF_INET
        tmp = socket.socket(family, socket.SOCK_STREAM)
        tmp.bind((self.host, 0))
        self.port = tmp.getsockname()[1]
        tmp.close()
        self.server = gevent.server.StreamServer((self.host, self.port), self._handle_conn)
        self.server.start()
        return self

    def __exit__(self, *_args):
        try:
            self.server.stop()
        except Exception:
            pass

    def wait_for_connection(self, timeout=2):
        try:
            self.queue.get(block=True, timeout=timeout)
            return True
        except Exception:
            return False


def _tls_enabled(env):
    """Return True when the running Redis was started with TLS enabled.
    The bug only manifests on the TLS code path."""
    try:
        res = env.cmd('CONFIG', 'GET', 'tls-port')
    except Exception:
        return False
    if not res or len(res) < 2:
        return False
    try:
        return int(res[1]) != 0
    except (TypeError, ValueError):
        return False


def _alive(env):
    """Return True iff the underlying Redis process answered a PING."""
    try:
        env.cmd('PING')
    except Exception:
        return False
    return True


def _push_topology(env, host, mock_port):
    """Tell libmr that there is a second shard listening on the mock peer."""
    promote_internal_client_if_supported(env=env)
    endpoint_host = '[%s]' % host if ':' in host else host
    args = [
        'NO-USED', 'NO-USED', 'NO-USED', 'NO-USED', 'NO-USED',
        '1',
        'RANGES', '2',
        'SHARD', '1',
        'SLOTRANGE', '0', '8192',
        'ADDR', 'password@%s:6379' % endpoint_host,
        'MASTER',
        'SHARD', '2',
        'SLOTRANGE', '8193', '16383',
        'ADDR', 'password@%s:%d' % (endpoint_host, mock_port),
        'MASTER',
    ]
    env.cmd('MRTESTS.CLUSTERSET', *args)
    env.cmd('MRTESTS.FORCESHARDSCONNECTION')


@MRTestDecorator(skipOnCluster=True)
def testNoCrashOnTLSHandshakeFailure(env, conn):
    if not _tls_enabled(env):
        raise unittest.SkipTest('test requires --tls')

    for host in _get_hosts():
        with _RejectingShard(host) as mock:
            # Drive several connection cycles. Each cycle exercises the path:
            #   connect -> TLS handshake -> SSL_connect EOF -> schedule
            #   MR_ClusterAsyncDisconnect -> hiredis disconnect callback nulls
            #   n->c -> deferred task runs redisAsyncFree(NULL) -> CRASH.
            # The first failed cycle is usually enough to crash an unfixed
            # build; we loop to be robust under timing jitter.
            for _ in range(20):
                _push_topology(env, host, mock.port)
                # Wait for libmr to actually attempt the connection so the
                # event-loop thread has time to run the SSL handshake and the
                # deferred MR_ClusterAsyncDisconnect task.
                mock.wait_for_connection(timeout=2)
                time.sleep(0.05)
                # If the bug fired, this PING raises (connection refused or
                # broken pipe) because the Redis process is dead. We only care
                # that the server is alive; some redis-py versions return
                # True for PING, others 'PONG' / b'PONG'.
                env.assertTrue(_alive(env),
                               message='Redis crashed after TLS handshake failure')

        # Final liveness check after the mock has been torn down.
        env.assertTrue(_alive(env))
