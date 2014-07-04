import gevent

import nsq.master
import nsq.node_collection


class Producer(nsq.master.Master):
    def __init__(self, topic, node_collection, *args, **kwargs):
        # The producer can only interact with consumers.
        assert issubclass(
                node_collection.__class__, 
                nsq.node_collection.ConsumerNodes) is True

        super(Producer, self).__init__(*args, **kwargs)

        self.__topic = topic
        self.__node_collection = node_collection

    def start(self, *args, **kwargs):
        nodes = self.__node_collection.get_servers(self.__topic)
        self.set_servers(nodes)

        # This will launch the master connection manager into its own greenlet 
        # so that we can return.
        gevent.spawn(super(Producer, self).run, *args, **kwargs)
