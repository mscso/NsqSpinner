import time
import logging

import gevent
import gevent.socket

import nsq.config
import nsq.exceptions

_logger = logging.getLogger(__name__)


class Node(object):
    def __init__(self, server_host):
        self.__server_host = server_host

    def __hash__(self):
        return ('server', self.__server_host).__hash__()

    def __eq__(self, o):
        if o is None:
            return False
        elif self.__class__ != o.__class__:
            return False
        elif self.__server_host != o.__server_host:
            return False

        return True

    def __ne__(self, o):
        return not (self == o)

    def __str__(self):
        return ('<NODE [%s] [%s]>' % 
                (self.__class__.__name__, self.__server_host))

    def connect(self, nice_quit_ev):
        raise NotImplementedError()

# TODO(dustin): This version always blocks for five-seconds.
#
#    def primitive_connect(self):
#        return gevent.socket.create_connection(
#                self.server_host)
#
    def primitive_connect(self):
        s = gevent.socket.socket(
                gevent.socket.AF_INET, 
                gevent.socket.SOCK_STREAM)

        s.connect(self.server_host)

        return s

    @property
    def server_host(self):
        return self.__server_host


class DiscoveredNode(Node):
    """Represents a node that we found via lookup servers."""

    def connect(self, nice_quit_ev):
        """Connect the server. We expect this to implement backoff and all 
        connection logistics for servers that were discovered via a lookup 
        node.
        """ 

        _logger.debug("Connecting to discovered node: [%s]", self.server_host)

        stop_epoch = time.time() + \
                        nsq.config.client.MAXIMUM_CONNECT_ATTEMPT_PERIOD_S

        timeout_s = nsq.config.client.INITIAL_CONNECT_FAIL_WAIT_S
        backoff_rate = nsq.config.client.CONNECT_FAIL_WAIT_BACKOFF_RATE

        while stop_epoch >= time.time() and nice_quit_ev.is_set() is False:
            try:
                c = self.primitive_connect()
            except gevent.socket.error:
                _logger.exception("Could not connect to discovered server: "
                                  "[%s]", self.server_host)
            else:
                _logger.info("Discovered server-node connected: [%s]", 
                             self.server_host)
                
                return c

            timeout_s = min(timeout_s * backoff_rate,
                            nsq.config.client.MAXIMUM_CONNECT_FAIL_WAIT_S)

            _logger.info("Waiting for (%d) seconds before reconnecting.", 
                         timeout_s)

            gevent.sleep(timeout_s)

        raise nsq.exceptions.NsqConnectGiveUpError(
                "Could not connect to the nsqlookupd server: [%s]" % 
                (self.server_host,))


class ServerNode(Node):
    """Represents a node that was explicitly prescribed."""

    def connect(self, nice_quit_ev):
        """Connect the server. We expect this to implement connection logistics 
        for servers that were explicitly prescribed to us.
        """ 

        _logger.debug("Connecting to explicit server node: [%s]", 
                      self.server_host)

        # According to the docs, a nsqlookupd-discovered server should fall-out 
        # of the lineup immediately if it fails. If it comes back, nsqlookupd 
        # will give it back to us.

        try:
            c = self.primitive_connect()
        except gevent.socket.error:
            _logger.exception("Could not connect to explicit server: [%s]",
                              self.server_host)

            raise nsq.exceptions.NsqConnectGiveUpError(
                    "Could not connect to the nsqd server: [%s]" % 
                    (self.server_host,))

        _logger.info("Explicit server-node connected: [%s]", self.server_host)
        return c
