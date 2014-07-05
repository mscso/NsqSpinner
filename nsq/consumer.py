import types

import gevent

import nsq.master
import nsq.node_collection
import nsq.command


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

    def __initialize_connection(self, duty, rdy, connection):
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

# TODO(dustin): We seem to only be receiving the first message, and not 
#               subsequent ones... Even though we're marking them as finished. 
#               Is this some kind of counter that has to be reset when/before 
#               it reaches (0).
        try:
            rdy_this = rdy(connection.node, self)
        except TypeError:
            rdy_this = rdy

        command.rdy(rdy_this)

    def run(self, duty, rdy, *args, **kwargs):
        using_lookup = issubclass(
                        self.__node_collection.__class__, 
                        nsq.node_collection.LookupNodes)

        # Get a list of servers and schedule future checks (if we were given
        # lookup servers).
        self.__discover(using_lookup)

        super(Consumer, self).run(*args, **kwargs)

# TODO(dustin): We'll need to apply some series of startup events to every new 
#               or reestablished connection.
        for c in self.connections:
            self.__initialize_connection(duty, rdy, c)

        # Block until we're told to terminate.
        self.terminate_ev.wait()
