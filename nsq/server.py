import nsq.master
import nsq.node_collection


class Consumer(nsq.master.Master):
    def __init__(self, topic, channel, node_collection, 
                 *args, **kwargs):
        # The server, by the spec, only connects to server nodes.
        assert issubclass(
                node_collection.__class__, 
                nsq.node_collection.ServerNodes) is True

        super(Server, self).__init__(node_collection, 
                                     *args, **kwargs)

        self.__topic = topic
        self.__channel = channel
