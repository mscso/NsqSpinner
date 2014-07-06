import logging
import types
import functools

import gevent

import nsq.master
import nsq.node_collection
import nsq.command
import nsq.connection_callbacks
import nsq.connection

_logger = logging.getLogger(__name__)


class _ConnectionCallbacks(object):
    def __init__(self, original_ccallbacks, duty, rdy, master, 
                 connection_context):
        self.__original_ccallbacks = original_ccallbacks
        self.__duty = duty
        self.__rdy = rdy
        self.__master = master
        self.__connection_context = connection_context

    def __send_sub(self, connection, command):
        """Duty can be a tuple of topic and channel. If it's a 
        callback, call it to get the topic and channel for this 
        connection. The callback can get the total number of 
        connections via this object.
        """

        try:
            duty_this = self.__duty(connection.node, self.__master)
        except TypeError:
            duty_this = self.__duty

        command.sub(duty_this[0], duty_this[1])

    def __send_rdy(self, connection, command):
        """We determine the RDY count in the same fashion as the duty.
        """

        try:
            rdy_this = self.__rdy(connection.node, self.__master)
        except TypeError:
            rdy_this = self.__rdy

        command.rdy(rdy_this)

        return rdy_this

    def __initialize_connection(self, connection):
        _logger.debug("Initializing connection: [%s]", connection.node)

        command = nsq.command.Command(connection)

        self.__send_sub(connection, command)
        rdy = self.__send_rdy(connection, command)

        self.__connection_context[connection] = { 'rdy_count': rdy }

    def connect(self, connection):
        self.__initialize_connection(connection)

        self.__original_ccallbacks.connect(connection)

    def broken(self, connection):
        del self.__connection_context[connection]

        self.__original_ccallbacks.broken(connection)

    def message_received(self, connection, message):
        self.__connection_context[connection]['rdy_count'] -= 1

        if self.__connection_context[connection]['rdy_count'] <= 0:
            _logger.info("RDY count has reached zero for [%s]. Re-"
                         "setting.", connection)

            command = nsq.command.Command(connection)
            rdy = self.__send_rdy(connection, command)
# TODO(dustin): This might need more smarts. This will probably have to be an 
#               actively-adjusted value.
            self.__connection_context[connection]['rdy_count'] = rdy

        self.__original_ccallbacks.message_received(connection, message)

    def __getattr__(self, name):
        """Forward all other callbacks to the original. In order for 
        this to work, we didn't inherit from anything.
        """

        return getattr(self.__original_ccallbacks, name)


class Consumer(nsq.master.Master):
    def __init__(self, topic, channel, node_collection,
                 tls_ca_bundle_filepath=None, tls_auth_pair=None, 
                 compression=False, *args, **kwargs):
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
        self.__connection_context = {}
        self.__is_tls = bool(tls_ca_bundle_filepath or tls_auth_pair)
        self.__tls_ca_bundle_filepath = tls_ca_bundle_filepath
        self.__tls_auth_pair = tls_auth_pair
        self.__compression = compression

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

        if self.__is_tls is True:
            if self.__tls_ca_bundle_filepath is None:
                raise ValueError("Please provide a CA bundle.")

            nsq.connection.TLS_CA_BUNDLE_FILEPATH = self.__tls_ca_bundle_filepath
            nsq.connection.TLS_AUTH_PAIR = self.__tls_auth_pair
            self.identify.set_tls_v1()

        if self.__compression is True:
            self.identify.set_snappy()

        using_lookup = issubclass(
                        self.__node_collection.__class__, 
                        nsq.node_collection.LookupNodes)

        # Get a list of servers and schedule future checks (if we were given
        # lookup servers).

        self.__discover(using_lookup)

        # Preempt the callbacks that may have been given to us in order to 
        # keep our consumer in order.

        cc = _ConnectionCallbacks(
                ccallbacks, 
                duty,
                rdy,
                self, 
                self.__connection_context)

        super(Consumer, self).run(ccallbacks=cc)

        # Block until we're told to terminate.
        self.terminate_ev.wait()
