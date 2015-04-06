import logging
import collections

import gevent
import gevent.event

import nsq.config.client
import nsq.exceptions
import nsq.node_collection
import nsq.connection
import nsq.identify
import nsq.connection_election
import nsq.message_handler

_logger = logging.getLogger(__name__)

NODE_CONTEXT = collections.namedtuple('NodeContext', ['topic', 'channel'])
NODE_COUPLET = collections.namedtuple('NodeCouplet', ['context', 'node'])


class Master(object):
    """This class is responsible to orchestrating connections and greenlets 
    common to both producers and consumers.
    """

    def __init__(self, connection_ignore_quit=False, message_handler_cls=None):
        assert message_handler_cls is None or \
               issubclass(
                message_handler_cls, 
                nsq.message_handler.MessageHandler) is True

        self.__connection_ignore_quit = connection_ignore_quit
        self.__message_handler_cls = message_handler_cls

        self.__node_couplets_s = set()
        self.__identify = nsq.identify.Identify().set_feature_negotiation()
        self.__connections = []
        self.__ready_ev = gevent.event.Event()
        self.__election = nsq.connection_election.ConnectionElection(self)
        self.__quit_ev = gevent.event.Event()
        self.__is_alive = False
        self.__message_handler = None

    def __start_connection(self, context, node, ccallbacks=None):
        """Start a new connection, and manage it from a new greenlet."""

        _logger.debug("Creating connection object: CONTEXT=[%s] NODE=[%s]", 
                      context, node)

        c = nsq.connection.Connection(
                context,
                node, 
                self.__identify, 
                self.__message_handler,
                self.__quit_ev,
                ccallbacks,
                ignore_quit=self.__connection_ignore_quit)

        g = gevent.spawn(c.run)

        # Now, wait for the thread to finish the connection.

        timeout_s = nsq.config.client.NEW_CONNECTION_NEGOTIATE_TIMEOUT_S
        if c.connected_ev.wait(timeout_s) is False:
            _logger.error("New connection to server [%s] timed-out. Cleaning-"
                          "up thread.", node)

            g.kill()
            g.join()

            # We'll try again on the next audit.

            raise EnvironmentError("Connection to server [%s] failed." % 
                                   (node,))

        self.__connections.append((node, c, g))

    def __wait_for_one_server_connection(self):
        """Wait until at least one server is connected. Since quitting relies 
        on a bunch of loops terminating, attempting to quit [cleanly] 
        immediately will still have to wait for the connections to finish 
        starting.
        """

        _logger.info("Waiting for first connection.")

        while 1:
            is_connected_to_one = False
            for (n, c, g) in self.__connections:
                if c.is_connected is True:
                    is_connected_to_one = True
                    break
                elif g.exception == nsq.exceptions.NsqConnectGiveUpError:
                    raise IOError("One of the servers could not be connected "
                                  "during startup: [%s]" % (c))
                elif g.exception is not None:
                    raise IOError("One of the connection gthreads had an "
                                  "uncaught exception during startup: [%s] "
                                  "[%s]" % 
                                  (g.exception.__class__.__name__, 
                                   str(g.exception)))
                elif g.dead is True:
                    raise SystemError("One of the connection gthreads died "
                                      "during startup: [%s]" % (c,))

            if is_connected_to_one is True:
                break

            gevent.sleep(nsq.config.client.CONNECT_AUDIT_WAIT_INTERVAL_S)

    def __audit_connections(self, ccallbacks):
        """Monitor state of all connections, and utility of all servers."""

        while self.__quit_ev.is_set() is False:
            # Remove any connections that are dead.
            self.__connections = filter(
                                    lambda (n, c, g): not g.ready(), 
                                    self.__connections)

            connected_node_couplets_s = set([
                (c.managed_connection.context, node)
                for (node, c, g) 
                in self.__connections])

            # Warn if there are any still-active connections that are no longer 
            # being advertised (probably where we were given some lookup servers 
            # that have dropped this particular *nsqd* server).

            lingering_nodes_s = connected_node_couplets_s - \
                                self.__node_couplets_s

            if lingering_nodes_s:
                _logger.warning("Server(s) are connected but no longer "
                                "advertised: %s", lingering_nodes_s)

            # Connect any servers that don't currently have a connection.

            unused_nodes_s = self.__node_couplets_s - connected_node_couplets_s

            for (context, node) in unused_nodes_s:
                _logger.info("Trying to connect unconnected server: "
                             "CONTEXT=[%s] NODE=[%s]", context, node)

                self.__start_connection(context, node, ccallbacks)
            else:
                # Are there both no unused servers and no connected servers?
                if not connected_node_couplets_s:
                    _logger.error("All servers have gone away. Stopping "
                                  "client.")

                    # Clear our list of servers, and squash the "no servers!" 
                    # error so that we can shut things down in the right order.

                    try:
                        self.set_servers([])
                    except EnvironmentError:
                        pass

                    self.__quit_ev.set()
                    return

            interval_s = \
                nsq.config.client.GRANULAR_CONNECTION_AUDIT_SLEEP_STEP_TIME_S

            audit_wait_s = float(nsq.config.client.CONNECTION_AUDIT_WAIT_S)

            while audit_wait_s > 0 and\
                  self.__quit_ev.is_set() is False:
                gevent.sleep(interval_s)
                audit_wait_s -= interval_s

    def __join_connections(self):
        """Wait for all connections to close. There are no side-effects here. 
        We just want to try and leave -after- everything has closed, in 
        general.
        """

        interval_s = nsq.config.client.CONNECTION_CLOSE_AUDIT_WAIT_S
        graceful_wait_s = nsq.config.client.CONNECTION_QUIT_CLOSE_TIMEOUT_S
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

    def __manage_connections(self, ccallbacks=None):
        """This runs as the main connection management greenlet."""

        _logger.info("Running client.")

        # Create message-handler.

        if self.__message_handler_cls is not None:
# TODO(dustin): Move this to another thread if we can mix multithreading with coroutines. 
            self.__message_handler = self.__message_handler_cls(
                                        self.__election, 
                                        ccallbacks)

        # Spawn the initial connections to all of the servers.

        for (context, node) in self.__node_couplets_s:
            self.__start_connection(context, node, ccallbacks)

        # Wait for at least one connection to the server.
        self.__wait_for_one_server_connection()

        # Indicate that the client is okay to pass control back to the caller.
        self.__is_alive = True
        self.__ready_ev.set()

        # Loop, and maintain all connections. This exits when the quit event 
        # is set.
        self.__audit_connections(ccallbacks)

        # Wait for all of the connections to close. They will respond to the 
        # same quit event that terminate the audit loop just above.
        self.__join_connections()

        _logger.info("Connection management has stopped.")

        self.__is_alive = False

    def set_servers(self, node_couplets):
        """Set the current collection of servers. The entries are 2-tuples of 
        contexts and nodes.
        """

        node_couplets_s = set(node_couplets)

        if node_couplets_s != self.__node_couplets_s:
            _logger.info("Servers have changed. NEW: %s REMOVED: %s", 
                         node_couplets_s - self.__node_couplets_s, 
                         self.__node_couplets_s - node_couplets_s)

        # Since no servers means no connection greenlets, and the discover 
        # greenlet is technically scheduled and not running between 
        # invocations, this should successfully terminate the process.
        if not node_couplets_s:
            raise EnvironmentError("No servers available.")

        self.__node_couplets_s = node_couplets_s

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
        """Stop all of the connections."""

        _logger.debug("Emitting quit signal for connections.")
        self.__quit_ev.set()

        _logger.info("Waiting for connection manager to stop.")
        self.__manage_g.join()

    def get_node_count_for_topic(self, topic):
        return len(filter(
                    lambda nc: nc.context.topic == topic, 
                    self.__node_couplets_s))

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
    def is_alive(self):
        """If the client is still healthy and active."""

        return self.__is_alive

    @property
    def connection_election(self):
        """Expose a connection-election object."""

        return self.__election
