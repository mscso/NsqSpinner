import logging
import types
import functools

import gevent

import nsq.master
import nsq.node_collection
import nsq.command
import nsq.connection_callbacks

_logger = logging.getLogger(__name__)


class Consumer(nsq.master.Master):
    def __init__(self, topic, channel, node_collection, 
                 *args, **kwargs):
        # The consumer can interact either with producers or lookup servers 
        # (which render producers).
        assert issubclass(
                node_collection.__class__, 
                (nsq.node_collection.ProducerNodes, 
                 nsq.node_collection.LookupNodes)) is True

        super(Consumer, self).__init__(*args, **kwargs)

        self.__node_collection = node_collection
        self.__topic = topic
        self.__channel = channel

    def __discover(self, schedule_again):
        """This runs in its own greenlet, and maintains a list of servers."""
# TODO(dustin): We might want to allow for a set of different topics, and to 
#               make connections on behalf of all of them.
        nodes = self.__node_collection.get_servers(self.__topic)
        self.set_servers(nodes)

        if schedule_again is True:
            gevent.spawn_later(
                nsq.config.client.LOOKUP_READ_INTERVAL_S,
                self.__discover,
                True)

    def run(self, duty, rdy, ccallbacks=None):
        if ccallbacks is None:
            ccallbacks = nsq.connection_callbacks.ConnectionCallbacks()

        using_lookup = issubclass(
                        self.__node_collection.__class__, 
                        nsq.node_collection.LookupNodes)

        # Get a list of servers and schedule future checks (if we were given
        # lookup servers).
        self.__discover(using_lookup)

        def initialize_connection(duty, rdy, connection):
            _logger.debug("Initializing connection: [%s]", connection.node)

            command = nsq.command.Command(connection)

            # Duty can be a tuple of topic and channel. If it's a callback,
            # call it to get the topic and channel for this connection. The
            # callback can get the total number of connections via this object.

            try:
                duty_this = duty(connection.node, self)
            except TypeError:
                duty_this = duty

            command.sub(duty_this[0], duty_this[1])

            # We determine the RDY count in the same fashion as the duty.

            try:
                rdy_this = rdy(connection.node, self)
            except TypeError:
                rdy_this = rdy

            command.rdy(rdy_this)

        connect_cb_original = ccallbacks.connect

        def connect_cb(connection):
            initialize_connection(duty, rdy, connection)

            connect_cb_original(connection)

        ccallbacks.connect = connect_cb

        super(Consumer, self).run(ccallbacks=ccallbacks)

        # Block until we're told to terminate.
        self.terminate_ev.wait()
