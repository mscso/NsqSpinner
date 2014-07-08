import logging
import gevent

import nsq.master
import nsq.node_collection
import nsq.connection
import nsq.connection_election

_logger = logging.getLogger(__name__)


class Producer(nsq.master.Master):
    def __init__(self, topic, node_collection, tls_ca_bundle_filepath=None, 
                 tls_auth_pair=None, compression=False, identify=None, 
                 *args, **kwargs):
        # The consumer can interact either with producers or lookup servers 
        # (which render producers).
        assert issubclass(
                node_collection.__class__, 
                nsq.node_collection.ConsumerNodes) is True

        super(Producer, self).__init__(*args, **kwargs)

        self.__topic = topic

        is_tls = bool(tls_ca_bundle_filepath or tls_auth_pair)

        if is_tls is True:
            if tls_ca_bundle_filepath is None:
                raise ValueError("Please provide a CA bundle.")

            nsq.connection.TLS_CA_BUNDLE_FILEPATH = tls_ca_bundle_filepath
            nsq.connection.TLS_AUTH_PAIR = tls_auth_pair
            self.identify.set_tls_v1()

        if compression is True:
            self.identify.set_snappy()

        nodes = node_collection.get_servers(topic)
        self.set_servers(nodes)

        # If we we're given an identify instance, apply our apply our identify 
        # defaults them, and then replace our identify values -with- them (so we 
        # don't lose the values that we set, but can allow them to set everything 
        # else). 
        if identify is not None:
            identify.update(self.identify.parameters)
            self.identify.update(identify.parameters)

        self.__ce = nsq.connection_election.ConnectionElection(self)

    def publish(self, message):
        self.__ce.elect_connection().pub(self.__topic, message)

    def mpublish(self, messages):
        self.__ce.elect_connection().mpub(self.__topic, messages)

    def finish_and_quit(self):
# TODO(dustin): Finish this. Only quit after we've cleared the queue and closed 
#               the connections (prevent new connections from being 
#               established, and emitting CLS on the existing connections.
        gevent.sleep(1)
