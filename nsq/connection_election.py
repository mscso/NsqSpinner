"""This file deals with the semantics of choosing who to send commands to."""


class ConnectionElection(object):
    def __init__(self, master):
        self.__master = master

    def elect_connection(self):
# TODO(dustin): Determine what connection to send the next command on. This 
#               should implement semantics to ensure fairness. Return a command
#               object.
#
#               We think RR over the existing non-sleeping connections should 
#               be fine. All connections that have an RDY > 0 are considered 
#               equal.
        for connection in self.__master.connections:
            return connection.command

    def command_for_all_connections(self, cb):
        """Invoke the callback with a command-object for each connection."""

        for connection in self.__master.connections:
            cb(connection.command)
