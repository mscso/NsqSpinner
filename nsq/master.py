import logging
import gevent
import gevent.event

import nsq.config.client
import nsq.node_collection
import nsq.connection
import nsq.identify
import nsq.connection_election

_logger = logging.getLogger(__name__)


class Master(object):
    def __init__(self, message_handler_cls=None):
        self.__nodes_s = set()
        self.__identify = nsq.identify.Identify().set_feature_negotiation()
        self.__connections = []
        self.__message_handler_cls = message_handler_cls
        self.__message_q = gevent.queue.Queue()
        self.__ready_ev = gevent.event.Event()
        self.__terminate_ev = gevent.event.Event()
        self.__election = nsq.connection_election.ConnectionElection(self)

    def __start_connection(self, node):
        _logger.debug("Creating connection object: [%s]", node)

        c = nsq.connection.Connection(
                node, 
                self.__identify, 
                self.__message_q)

        g = gevent.spawn(c.run)
        self.__connections.append((node, c, g))

    def __manage_connections(self):
        _logger.info("Running client.")

        # Spawn connections to all of the servers.

        for node in self.__nodes_s:
            self.__start_connection(node)

        # Wait until at least one server is connected.

        _logger.info("Waiting for first connection.")

        is_connected_to_one = False
        while 1:

            for (n, c, g) in self.__connections:
                if c.is_connected is True:
                    is_connected_to_one = True
                    break

            if is_connected_to_one is True:
                break

# TODO(dustin): Put this into config.
            gevent.sleep(.1)

        self.__ready_ev.set()

        # Spawn the message handler.

        message_handler = self.__message_handler_cls(self.__election)

        gevent.spawn(
            message_handler.run, 
            self.__message_q)

        # Loop, and maintain all connections.

# TODO(dustin): We should set terminate_ev when we've encountered a critical 
#               error and had to kill all of the greenlets.

        while 1:
            # Remove any connections that are dead.
            self.__connections = filter(
                                    lambda (n, c, g): not g.ready(), 
                                    self.__connections)

            connected_nodes_s = set([node 
                                     for (node, c, g) 
                                     in self.__connections])

            # Warn if there are any still-active connections that are no longer 
            # being advertised (probably where we were given some lookup servers 
            # that have dropped this particular *nsqd* server).

            lingering_nodes_s = connected_nodes_s - self.__nodes_s

            if lingering_nodes_s:
                _logger.warning("Server(s) are connected but no longer "
                                "advertised: %s", lingering_nodes_s)

            # Connect any servers that don't currently have a connection.

            unused_nodes_s = self.__nodes_s - connected_nodes_s

            for node in unused_nodes_s:
                _logger.info("We've received a new server.")
                self.__start_connection(node)
            else:
                # Are there both no unused servers and no connected servers?
                if not connected_nodes_s:
                    raise EnvironmentError("All servers have gone away.")

            gevent.sleep(nsq.config.client.CONNECTION_AUDIT_WAIT_S)

    def set_servers(self, nodes):
        nodes_s = set(nodes)

        if nodes_s != self.__nodes_s:
            _logger.info("Servers have changed. NEW: %s REMOVED: %s", 
                         nodes_s - self.__nodes_s, 
                         self.__nodes_s - nodes_s)

        # Since no servers means no connection greenlets, and the discover 
        # greenlet is technically scheduled and not running between 
        # invocations, this should successfully terminate the process.
        if not nodes_s:
            raise EnvironmentError("No servers available.")

        self.__nodes_s = nodes_s

    def run(self):
        """Establish and maintain connections."""

        gevent.spawn(self.__manage_connections)
        self.__ready_ev.wait()

    @property
    def identify(self):
        return self.__identify

    @property
    def connections(self):
        return (c.managed_connection for (n, c, g) in self.__connections)

    @property
    def connection_count(self):
        return len(self.__connections)

    @property
    def terminate_ev(self):
        return self.__terminate_ev
