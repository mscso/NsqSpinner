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
        self.__message_handler_cls = message_handler_cls
        self.__nodes_s = set()
        self.__identify = nsq.identify.Identify().set_feature_negotiation()
        self.__connections = []
        self.__message_q = gevent.queue.Queue()
        self.__ready_ev = gevent.event.Event()
        self.__election = nsq.connection_election.ConnectionElection(self)
        self.__quit_ev = gevent.event.Event()

    def __start_connection(self, node, ccallbacks=None):
        _logger.debug("Creating connection object: [%s]", node)

        c = nsq.connection.Connection(
                node, 
                self.__identify, 
                self.__message_q,
                self.__quit_ev,
                ccallbacks)

        g = gevent.spawn(c.run)
        self.__connections.append((node, c, g))

    def __manage_connections(self, ccallbacks=None):
        _logger.info("Running client.")

        # Spawn connections to all of the servers.

        for node in self.__nodes_s:
            self.__start_connection(node, ccallbacks)

        # Wait until at least one server is connected. Since quitting relies on 
        # a bunch of loops terminating, attempting to quit [cleanly] 
        # immediately will still have to wait for the connections to finish 
        # starting.

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

        if self.__message_handler_cls is not None:
            message_handler = self.__message_handler_cls(self.__election, ccallbacks)

            gevent.spawn(
                message_handler.run, 
                self.__message_q)

        # Loop, and maintain all connections.

# TODO(dustin): We should set quit_ev when we've encountered a critical 
#               error and had to kill all of the greenlets.

        while self.__quit_ev.is_set() is False:
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
                self.__start_connection(node, ccallbacks)
            else:
                # Are there both no unused servers and no connected servers?
                if not connected_nodes_s:
                    raise EnvironmentError("All servers have gone away.")


            interval_s = .5
            audit_wait_s = float(nsq.config.client.CONNECTION_AUDIT_WAIT_S)

            while audit_wait_s > 0:
                gevent.sleep(interval_s)
                audit_wait_s -= interval_s

        connection_greenlets = [g for (n, c, g) in self.__connections]

# TODO(dustin): Put these constants in the config.
        interval_s = .5
        graceful_wait_s = 5.0
        graceful = False

        while graceful_wait_s > 0:
            if not self.__connections:
                break

            connected_list = [c.is_connected for (n, c, g) in self.__connections]
            if any(connected_list) is False:
                graceful = True
                break

            # We need to give the greenlets periodic control, in order to finish 
            # up.

            gevent.sleep(interval_s)
            graceful_wait_s -= interval_s

        if graceful is False:
            connected_list = [c for (n, c, g) in self.__connections if c.is_connected]
            _logger.error("We were told to terminate, but not all "
                          "connections were stopped: [%s]", connected_list)

        _logger.info("Connection management has stopped.")

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

    def set_compression(self, specific=None):
        if specific == 'snappy' or specific is None:
            self.identify.set_snappy()
        elif specific == 'deflate':
            self.identify.set_deflate()
        else:
            raise ValueError("Compression scheme [%s] not valid." % 
                             (specific,))

    def start(self, ccallbacks=None):
        """Establish and maintain connections."""

        self.__manage_g = gevent.spawn(self.__manage_connections, ccallbacks)
        self.__ready_ev.wait()

    def stop(self):
        _logger.debug("Emitting quit signal to consumer greenlets.")
        self.__quit_ev.set()

        _logger.info("Waiting for consumer workings to stop.")
        self.__manage_g.join()

    @property
    def identify(self):
        return self.__identify

    @property
    def connections(self):
        return (c.managed_connection for (n, c, g) in self.__connections)

    @property
    def connection_count(self):
        """This describes the connection-greenlets that we've spawned or 
        connections that we've actually established.
        """
        
        return len(self.__connections)

    @property
    def nodes_s(self):
        """This describes the servers that we know about."""
        return self.__nodes_s
