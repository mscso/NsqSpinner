import logging
import math
import random

import gevent
import gevent.event

import nsq.master
import nsq.node_collection
import nsq.command
import nsq.connection_callbacks
import nsq.connection

_logger = logging.getLogger(__name__)

# TODO(dustin): We still need to consider "backoff" from the perspective of
#               message processing (the "Backoff" section):
#
#               Exponential Backoff - when message processing fails the reader 
#               library will delay the receipt of additional messages for a 
#               duration that scales exponentially based on the # of 
#               consecutive failures. The opposite sequence happens when a 
#               reader is in a backoff state and begins to process 
#               successfully, until 0.


class ConsumerCallbacks(nsq.connection_callbacks.ConnectionCallbacks):
    def __init__(self, *args, **kwargs):
        super(ConsumerCallbacks, self).__init__(*args, **kwargs)

        self.__consumer = None
        self.__logger_rdy = _logger.getChild('rdy')
#        self.__logger_rdy.setLevel(logging.DEBUG)

    def set_consumer(self, consumer):
        self.__consumer = consumer

    def __send_sub(self, connection, command):
        command.sub(connection.topic, connection.channel)

    def __send_rdy(self, connection, command):
        """Determine the RDY value, and set it. It can either be a static value
        a callback, or None. If it's None, we'll calculate the value based on
        our limits and connection counts.

        The documentation recommends starting with (1), but since we are always
        dealing directly with *nsqd* servers by now, we'll always have a valid
        count to work with. Since we derive this count off a set of servers 
        that will always be up-to-date, we have everything we need, here, going
        forward.
        """

        if self.__consumer.original_rdy is None:
            node_count = self.__consumer.get_node_count_for_topic(
                            connection.topic)

            self.__logger_rdy.debug("Calculating RDY: max_in_flight=(%d) "
                                    "node_count=(%d)", 
                                    self.__consumer.max_in_flight, node_count)

            if self.__consumer.max_in_flight >= node_count:
                # Calculate the RDY based on the max_in_flight and total number 
                # of servers. We always round up, or else we'd run the risk of 
                # not facilitating some servers.
                rdy_this = int(math.ceil(
                                        float(self.__consumer.max_in_flight) /
                                        float(node_count)))

                self.__logger_rdy.debug("Assigning RDY based on max_in_flight "
                                        "(%d) and node count (%d) (optimal): "
                                        "(%d)", 
                                        self.__consumer.max_in_flight, 
                                        node_count, rdy_this)
            else:
                # We have two possible scenarios:
                # (1) The client is starting up, and the total RDY count is 
                #     already accounted for.
                # (2) The client is already started, and another connection has
                #     a (0) RDY count.
                #
                # In the case of (1), we'll take an RDY of (0). In the case of
                # (2) We'll send an RDY of (1) on their behalf, before we 
                # assume a (0) for ourself.

                # Look for existing connections that have a (0) RDY (which 
                # would've only been set to (0) intentionally).

                self.__logger_rdy.debug("(max_in_flight > nodes). Doing RDY "
                                        "election.")

                sleeping_connections = [
                    c \
                    for (c, info) \
                    in self.__consumer.connection_context.items() \
                    if info['rdy_count'] == 0]

                self.__logger_rdy.debug("Current sleeping_connections: %s", 
                                        sleeping_connections)

                if sleeping_connections:
                    elected_connection = random.choice(sleeping_connections)
                    self.__logger_rdy.debug("Sending RDY of (1) on: [%s]", 
                                            elected_connection)

                    command_elected = nsq.command.Command(elected_connection)
                    command_elected.rdy(1)
                else:
                    self.__logger.debug("No sleeping connections. We got the "
                                        "short stick: [%s]", connection)

                rdy_this = 0
        else:
            try:
                rdy_this = self.__consumer.original_rdy(
                            connection.node, 
                            self.__consumer.connection_count, 
                            self.__consumer)

                self.__logger_rdy.debug("Using RDY from callback: (%d)", 
                                        rdy_this)
            except TypeError:
                rdy_this = self.__consumer.original_rdy
                self.__logger_rdy.debug("Using static RDY: (%d)", rdy_this)

        # Make sure that the aggregate set of RDY counts doesn't exceed the 
        # max. This constrains the previous value, above.
        rdy_this = min(rdy_this + \
                        self.__get_total_rdy_count(), 
                       self.__consumer.max_in_flight)

        # Make sure we don't exceed the maximum specified by the server. This 
        # only works because we're running greenlets, not threads. At any given 
        # time, only one greenlet is running, and we can make sure to 
        # distribute the remainder of (max_in_flight / nodes) across a subset 
        # of the nodes (they don't all have to have an even slice of 
        # max_in_flight).

        server_features = self.__consumer.identify.server_features
        max_rdy_count = server_features['max_rdy_count']
        rdy_this = min(max_rdy_count, rdy_this)

        self.__logger_rdy.debug("Final RDY (max_in_flight=(%d) "
                                "max_rdy_count=(%d)): (%d)", 
                                self.__consumer.max_in_flight, max_rdy_count, 
                                rdy_this)

        if rdy_this > 0:
            command.rdy(rdy_this)
        else:
            self.__logger_rdy.info("This connection will go to sleep (not "
                                   "enough RDY to go around).")

        return rdy_this

    def __get_total_rdy_count(self):
        connection_context_values = self.__consumer.connection_context.values()
        counts = [c['rdy_count'] for c in connection_context_values]
        return sum(counts)

    def __initialize_connection(self, connection):
        _logger.debug("Initializing connection: [%s]", connection.node)

        command = nsq.command.Command(connection)

        self.__send_sub(connection, command)
        rdy = self.__send_rdy(connection, command)

        self.__consumer.connection_context[connection] = { 
            'rdy_count': rdy,
            'rdy_original': rdy,
        }

    def identify(self, connection):
        super(ConsumerCallbacks, self).identify(connection)

        self.__initialize_connection(connection)

    def broken(self, connection):
        super(ConsumerCallbacks, self).broken(connection)

        del self.__consumer.connection_context[connection]

    def rdy_replenish(self, connection, current_rdy, original_rdy):
        command = nsq.command.Command(connection)
        rdy = self.__send_rdy(connection, command)

        self.__consumer.connection_context[connection]['rdy_count'] = rdy

    def message_received(self, connection, message):
        super(ConsumerCallbacks, self).message_received(connection, message)

        self.__consumer.connection_context[connection]['rdy_count'] -= 1

        original_rdy = self.__consumer.connection_context[connection]['rdy_original']
        current_rdy = self.__consumer.connection_context[connection]['rdy_count']

        repost_threshold = original_rdy // 4
        if current_rdy <= repost_threshold:
            _logger.debug("RDY count has reached a depletion threshold for "
                          "[%s]. Re-setting.", connection)

            self.rdy_replenish(connection, current_rdy, original_rdy)

    @property
    def consumer(self):
        return self.__consumer


# TODO(dustin): We need to be able to subscribe to multiple topics/channels.
#
# 1. For each topic, do a lookup of servers.
# 2. Maintain a catalog of which topics go to which servers, fed by the 
#    nsqlookup servers.)
# 3. Our nodes_s set should have (server, topic) members, whether we get the 
#    servers from nsqlookupd or were given a list of nsqd servers rather than 
#    nsqlookup servers
# 4. When we initialize a connection, we'll need a topic name, too (only the
#    connection routine knows which topic that connection is supposed to
#    subscribe to).


class Consumer(nsq.master.Master):
    """Main consumer process.
    context_list is a list of 2-tuples (topic, channel).
    """

    def __init__(self, context_list, node_collection, max_in_flight, 
                 ccallbacks=None, rdy=None, tls_ca_bundle_filepath=None, 
                 tls_auth_pair=None, compression=False, identify=None, 
                 *args, **kwargs):
        super(Consumer, self).__init__(
            connection_ignore_quit=True, 
            *args, 
            **kwargs)

        # A consumer can interact either with nsqd or nsqlookupd servers 
        # (which render nsqd servers).
        assert issubclass(
                node_collection.__class__, 
                (nsq.node_collection.ServerNodes, 
                 nsq.node_collection.LookupNodes)) is True

        # Translate some of our parameters to IDENTIFY parameters.

        self.__configure_identify(
                self, 
                tls_ca_bundle_filepath, 
                tls_auth_pair, 
                compression, 
                identify)

        # Preempt the callbacks that may have been given to us in order to 
        # keep our consumer in order.

        if ccallbacks is None:
            cc = ConsumerCallbacks()
        else:
            assert issubclass(ccallbacks.__class__, ConsumerCallbacks)
            cc = ccallbacks

        cc.set_consumer(self)
        self.__cc = cc

        # Set local attributes.

        self.__context_list = context_list
        self.__connection_context = {}
        self.__max_in_flight = max_in_flight
        self.__original_rdy = rdy

        self.__quit_ev = gevent.event.Event()
        
        self.__node_collection = node_collection
        self.__consume_blocker_g = None

    def __configure_identify(self, m, tls_ca_bundle_filepath=None, 
                             tls_auth_pair=None, compression=None, 
                             identify=None):
        is_tls = bool(tls_ca_bundle_filepath or tls_auth_pair)

        if is_tls is True:
            if tls_ca_bundle_filepath is None:
                raise ValueError("Please provide a CA bundle.")

            nsq.connection.TLS_CA_BUNDLE_FILEPATH = tls_ca_bundle_filepath
            nsq.connection.TLS_AUTH_PAIR = tls_auth_pair
            m.identify.set_tls_v1()

        if compression:
            if compression is True:
                compression = None

            m.set_compression(compression)

        # If we we're given an identify instance, apply our apply our identify 
        # defaults them, and then replace our identify values -with- them (so we 
        # don't lose the values that we set, but can allow them to set everything 
        # else). 

        if identify is not None:
            identify.update(m.identify.parameters)
            m.identify.update(identify.parameters)

    def start(self):
        using_lookup = issubclass(
                        self.__node_collection.__class__, 
                        nsq.node_collection.LookupNodes)

        # Get a list of servers and schedule future checks (if we were given
        # lookup servers).

        def discover(schedule_again):
            """This runs in its own greenlet, and maintains a list of servers.
            """

            nodes = set()
            for topic, channel in self.__context_list:
                context = nsq.master.NODE_CONTEXT(topic, channel)
                context_nodes = [nsq.master.NODE_COUPLET(context, server) 
                                 for server 
                                 in self.__node_collection.get_servers(topic)]

                nodes.update(context_nodes)

            self.set_servers(nodes)

            if schedule_again is True:
                gevent.spawn_later(
                    nsq.config.client.LOOKUP_READ_INTERVAL_S,
                    discover,
                    True)

        # Establish a list of servers. Also schedule a next-check if we're 
        # using lookup servers.

        discover(using_lookup)

        # Start the master connection manager.

        super(Consumer, self).start(ccallbacks=self.__cc)

        # Now, spawn a greenlet to wait on the user to set the quit event.

        def consume_blocker():
            _logger.info("The master routine is now running. Blocking on quit "
                         "event.")

            self.__quit_ev.wait()

            _logger.info("Consumer is being stopped. Stopping master routine.")
            super(Consumer, self).stop()

        self.__consume_blocker_g = gevent.spawn(consume_blocker)

    def stop(self):
        _logger.debug("Setting quit event for the consumer.")
        self.__quit_ev.set()

        _logger.info("Asking server to close connections.")
        self.connection_election.command_for_all_connections(
                                    lambda command: command.cls())

        _logger.info("Waiting for the consumer to stop.")
        self.__consume_blocker_g.join()

        _logger.debug("Consumer stop complete.")

    @property
    def connection_context(self):
        return self.__connection_context

    @property
    def max_in_flight(self):
        return self.__max_in_flight

    @property
    def original_rdy(self):
        self.__original_rdy
