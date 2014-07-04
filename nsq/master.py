import logging
import gevent

import nsq.config.client
import nsq.node_collection
import nsq.connection
import nsq.identify

_logger = logging.getLogger(__name__)


class Master(object):
    def __init__(self):
        self.__nodes_s = set()
        self.__identify = nsq.identify.Identify().set_feature_negotiation()

    def run(self):
        """Establish and maintain connections."""

        _logger.info("Running client.")

        connections = []

        def start_connection(node):
            _logger.info("Starting connection to node: [%s]", node)

            c = nsq.connection.Connection(node, self.__identify)

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

    @property
    def identify(self):
        return self.__identify
