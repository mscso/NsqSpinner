import logging
import types
import functools
import math
import random

import gevent

import nsq.master
import nsq.node_collection
import nsq.command
import nsq.connection_callbacks
import nsq.connection

_logger = logging.getLogger(__name__)

# TODO(dustin): We still need to consider "backoff" from the perspective of
#               message processing (the "Backoff" section).


class _ConnectionCallbacks(object):
    def __init__(self, original_ccallbacks, topic, channel, master, 
                 connection_context, max_in_flight, rdy=None):
        self.__original_ccallbacks = original_ccallbacks
        self.__topic = topic
        self.__channel = channel
        self.__master = master
        self.__connection_context = connection_context
        self.__max_in_flight = max_in_flight
        self.__rdy = rdy

    def __send_sub(self, connection, command):
        command.sub(self.__topic, self.__channel)

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

        if self.__rdy is None:
            node_count = len(self.__master.nodes_s)

            _logger.debug("Calculating RDY: max_in_flight=(%d) node_count=(%d)", 
                          self.__max_in_flight, node_count)

            if self.__max_in_flight >= node_count:
                # Calculate the RDY based on the max_in_flight and total number of 
                # servers. We always round up, or else we'd run the risk of not 
                # facilitating some servers.
                rdy_this = int(math.ceil(
                                        float(self.__max_in_flight) /
                                        float(node_count)))

                _logger.debug("Assigning RDY based on max_in_flight (%d) and node "
                              "count (%d) (optimal): (%d)", 
                              self.__max_in_flight, node_count, rdy_this)
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

                _logger.debug("(max_in_flight > nodes). Doing RDY election.")

                sleeping_connections = [
                    c \
                    for (c, info) \
                    in self.__connection_context.items() \
                    if info['rdy_count'] == 0]

                _logger.debug("Current sleeping_connections: %s", 
                              sleeping_connections)

                if sleeping_connections:
                    elected_connection = random.choice(sleeping_connections)
                    _logger.debug("Sending RDY of (1) on: [%s]", 
                                  elected_connection)

                    command_elected = nsq.command.Command(elected_connection)
                    command_elected.rdy(1)
                else:
                    _logger.debug("No sleeping connections. We got the short "
                                  "stick: [%s]", connection)

                rdy_this = 0
        else:
            try:
                rdy_this = self.__rdy(
                            connection.node, 
                            self.__master.connection_count, 
                            self.__master)

                _logger.debug("Using RDY from callback: (%d)", rdy_this)
            except TypeError:
                rdy_this = self.__rdy
                _logger.debug("Using static RDY: (%d)", rdy_this)

        # Make sure that the aggregate set of RDY counts doesn't exceed the 
        # max. This constrains the previous value, above.
        rdy_this = min(rdy_this + \
                        self.__get_total_rdy_count(), 
                       self.__max_in_flight)

        # Make sure we don't exceed the maximum specified by the server. This 
        # only works because we're running greenlets, not threads. At any given 
        # time, only one greenlet is running, and we can make sure to 
        # distribute the remainder of (max_in_flight / nodes) across a subset 
        # of the nodes (they don't all have to have an even slice of 
        # max_in_flight).

        max_rdy_count = self.__master.identify.server_features['max_rdy_count']
        rdy_this = min(max_rdy_count, rdy_this)

        _logger.info("Final RDY (max_in_flight=(%d) max_rdy_count=(%d)): (%d)",
                     self.__max_in_flight, max_rdy_count, rdy_this)

        if rdy_this > 0:
            command.rdy(rdy_this)
        else:
            _logger.info("This connection will go to sleep (not enough RDY to "
                         "go around).")

        return rdy_this

    def __get_total_rdy_count(self):
        counts = [c['rdy_count'] for c in self.__connection_context.values()]
        return sum(counts)

    def __initialize_connection(self, connection):
        _logger.debug("Initializing connection: [%s]", connection.node)

        command = nsq.command.Command(connection)

        self.__send_sub(connection, command)
        rdy = self.__send_rdy(connection, command)

        self.__connection_context[connection] = { 
            'rdy_count': rdy,
            'rdy_original': rdy,
        }

    def connect(self, connection):
        self.__initialize_connection(connection)

        self.__original_ccallbacks.connect(connection)

    def broken(self, connection):
        del self.__connection_context[connection]

        self.__original_ccallbacks.broken(connection)

    def message_received(self, connection, message):
        self.__connection_context[connection]['rdy_count'] -= 1

        repost_threshold = \
            self.__connection_context[connection]['rdy_original'] // 4
        if self.__connection_context[connection]['rdy_count'] <= \
                repost_threshold:
            _logger.info("RDY count has reached zero for [%s]. Re-"
                         "setting.", connection)

            command = nsq.command.Command(connection)
            rdy = self.__send_rdy(connection, command)

            self.__connection_context[connection]['rdy_count'] = rdy

        self.__original_ccallbacks.message_received(connection, message)

    def __getattr__(self, name):
        """Forward all other callbacks to the original. In order for 
        this to work, we didn't inherit from anything.
        """

        return getattr(self.__original_ccallbacks, name)


def consume(topic, channel, node_collection, ccallbacks, max_in_flight, 
            rdy=None, tls_ca_bundle_filepath=None, tls_auth_pair=None, 
            compression=False, identify=None, *args, **kwargs):
    # The consumer can interact either with producers or lookup servers 
    # (which render producers).
    assert issubclass(
            node_collection.__class__, 
            (nsq.node_collection.ProducerNodes, 
             nsq.node_collection.LookupNodes)) is True

    m = nsq.master.Master(*args, **kwargs)

    if ccallbacks is None:
        ccallbacks = nsq.connection_callbacks.ConnectionCallbacks()

    connection_context = {}
    is_tls = bool(tls_ca_bundle_filepath or tls_auth_pair)

    if is_tls is True:
        if tls_ca_bundle_filepath is None:
            raise ValueError("Please provide a CA bundle.")

        nsq.connection.TLS_CA_BUNDLE_FILEPATH = tls_ca_bundle_filepath
        nsq.connection.TLS_AUTH_PAIR = tls_auth_pair
        m.identify.set_tls_v1()

    if compression is True:
        m.identify.set_snappy()

    using_lookup = issubclass(
                    node_collection.__class__, 
                    nsq.node_collection.LookupNodes)

    # Get a list of servers and schedule future checks (if we were given
    # lookup servers).

    def discover(schedule_again):
        """This runs in its own greenlet, and maintains a list of servers."""

        nodes = node_collection.get_servers(topic)
        m.set_servers(nodes)

        if schedule_again is True:
            gevent.spawn_later(
                nsq.config.client.LOOKUP_READ_INTERVAL_S,
                discover,
                True)

    discover(using_lookup)

    # Preempt the callbacks that may have been given to us in order to 
    # keep our consumer in order.

    cc = _ConnectionCallbacks(
            ccallbacks, 
            topic,
            channel,
            m, 
            connection_context,
            max_in_flight,
            rdy)

    # If we we're given an identify instance, apply our apply our identify 
    # defaults them, and then replace our identify values -with- them (so we 
    # don't lose the values that we set, but can allow them to set everything 
    # else). 
    if identify is not None:
        identify.update(m.identify.parameters)
        m.identify.update(identify.parameters)

    m.run(ccallbacks=cc)

    # Block until we're told to terminate.
    m.terminate_ev.wait()
