import logging
import gevent

import nsq.config.client
import nsq.node_collection
import nsq.connection

_logger = logging.getLogger(__name__)


class Client(object):
    def __init__(self, topic, channel, node_collection):
        assert issubclass(
                node_collection.__class__, 
                nsq.node_collection.Nodes) is True

        self.__topic = topic
        self.__channel = channel
        self.__node_collection = node_collection
        self.__nodes_s = set()

    def __discover(self, schedule_again):
        """This runs in its own greenlet, and maintains a list of servers."""

        nodes = self.__node_collection.get_servers(self.__topic)
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

        if schedule_again is True:
            gevent.spawn_later(
                nsq.config.client.LOOKUP_READ_INTERVAL_S,
                self.__discover,
                True)

    def run(self):
        """Establish and maintain connections."""

        _logger.info("Running client.")

        using_lookup = issubclass(
                        self.__node_collection.__class__, 
                        nsq.node_collection.LookupNodes)

        # Get a list of servers and schedule future checks (if we were given
        # lookup servers).
        self.__discover(using_lookup)

        connections = []

        def start_connection(node):
            _logger.info("Starting connection to node: [%s]", node)

            c = nsq.connection.Connection(node, self.__topic, self.__channel)
            g = gevent.spawn(c.run)
            connections.append((node, g))

        # Spawn connections to all of the servers.
        for node in self.__nodes_s:
            start_connection(node)

        # Loop, and maintain all connections.

        while 1:
            # Remove any connections that are dead.
            connections = filter(lambda (sh, g): not g.ready(), connections)

            connected_nodes_s = set([node for (node, g) in connections])

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
                start_connection(node)
            else:
                # Are there both no unused servers and no connected servers?
                if not connected_nodes_s:
                    raise EnvironmentError("All servers have gone away.")

            gevent.sleep(nsq.config.client.CONNECTION_AUDIT_WAIT_S)
