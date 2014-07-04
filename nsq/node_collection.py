import nsq.lookup
import nsq.node


class _Nodes(object):
    def get_servers(self, topic):
        raise NotImplementedError()


class _ServerNodes(_Nodes):
    def __init__(self, server_hosts):
        self.__server_hosts = server_hosts

    def get_servers(self, topic):
        return (nsq.node.ServerNode(sh) for sh in self.__server_hosts)


class ProducerNodes(_ServerNodes):
    """Used by a consumer to specify a producer NSQD collection."""

    pass


class ConsumerNodes(_ServerNodes):
    """Used by a producer to specify a consumer NSQD collection."""

    pass


class LookupNodes(_Nodes):
    """Used by a consumer to specify an NSQLOOKUPD collection, with which to 
    *derive* a set of NSQD servers.
    """

    def __init__(self, lookup_host_prefixes):
        self.__l = nsq.lookup.Lookup(lookup_host_prefixes)

    def get_servers(self, topic):
        server_hosts = self.__l.get_servers(topic)
        return (nsq.node.DiscoveredNode(sh) for sh in server_hosts)
