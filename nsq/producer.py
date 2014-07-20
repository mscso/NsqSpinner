import logging
import gevent

import nsq.master
import nsq.node_collection
import nsq.connection

_logger = logging.getLogger(__name__)


class Producer(nsq.master.Master):
    def __init__(self, topic, node_collection, tls_ca_bundle_filepath=None, 
                 tls_auth_pair=None, compression=False, identify=None, 
                 *args, **kwargs):
        # A producer can interact only with nsqd servers.
        assert issubclass(
                node_collection.__class__, 
                nsq.node_collection.ServerNodes) is True

        super(Producer, self).__init__(*args, **kwargs)

        self.__topic = topic

        is_tls = bool(tls_ca_bundle_filepath or tls_auth_pair)

        if is_tls is True:
            if tls_ca_bundle_filepath is None:
                raise ValueError("Please provide a CA bundle.")

            nsq.connection.TLS_CA_BUNDLE_FILEPATH = tls_ca_bundle_filepath
            nsq.connection.TLS_AUTH_PAIR = tls_auth_pair
            self.identify.set_tls_v1()

        if compression:
            if compression is True:
                compression = None

            self.set_compression(compression)

        nodes = node_collection.get_servers(topic)
        self.set_servers(nodes)

        # If we we're given an identify instance, apply our apply our identify 
        # defaults them, and then replace our identify values -with- them (so we 
        # don't lose the values that we set, but can allow them to set everything 
        # else). 
        if identify is not None:
            identify.update(self.identify.parameters)
            self.identify.update(identify.parameters)

    def publish(self, message):
        self.connection_election.elect_connection().pub(self.__topic, message)

    def mpublish(self, messages):
        self.connection_election.elect_connection().mpub(self.__topic, messages)
