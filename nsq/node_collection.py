import nsq.lookup
import nsq.node


class Nodes(object):
    def get_servers(self, topic):
        raise NotImplementedError()


class ServerNodes(Nodes):
    def __init__(self, server_hosts):
        self.__server_hosts = server_hosts

    def get_servers(self, topic):
        return (nsq.node.ServerNode(sh) for sh in self.__server_hosts)


class LookupNodes(Nodes):
    def __init__(self, lookup_host_prefixes):
        self.__l = nsq.lookup.Lookup(lookup_host_prefixes)

    def get_servers(self, topic):
        server_hosts = self.__l.get_servers(topic)
        return (nsq.node.DiscoveredNode(sh) for sh in server_hosts)
