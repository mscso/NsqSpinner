import gevent

import nsq.master
import nsq.node_collection


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

        nodes = self.__node_collection.get_servers(self.__topic)
        self.set_servers(nodes)

        if schedule_again is True:
            gevent.spawn_later(
                nsq.config.client.LOOKUP_READ_INTERVAL_S,
                self.__discover,
                True)

    def run(self, *args, **kwargs):
        using_lookup = issubclass(
                        self.__node_collection.__class__, 
                        nsq.node_collection.LookupNodes)

        # Get a list of servers and schedule future checks (if we were given
        # lookup servers).
        self.__discover(using_lookup)

        super(Consumer, self).run(*args, **kwargs)
