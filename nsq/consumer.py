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
    def __init__(self, original_ccallbacks, topic, channel, rdy, master, 
                 connection_context):
        self.__original_ccallbacks = original_ccallbacks
        self.__topic = topic
        self.__channel = channel
        self.__rdy = rdy
        self.__master = master
        self.__connection_context = connection_context

    def __send_sub(self, connection, command):
        command.sub(self.__topic, self.__channel)

    def __send_rdy(self, connection, command):
        try:
            rdy_this = self.__rdy(
                        connection.node, 
                        self.__master.connection_count, 
                        self.__master)
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


def consume(topic, channel, node_collection, rdy, ccallbacks,
            tls_ca_bundle_filepath=None, tls_auth_pair=None, 
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
            rdy,
            m, 
            connection_context)

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
