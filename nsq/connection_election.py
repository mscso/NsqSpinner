"""This file deals with the semantics of choosing who to send commands to."""

import logging

_logger = logging.getLogger(__name__)


class ConnectionElection(object):
    def __init__(self, master):
        self.__master = master
        self.__last_connection_name = None
        self.__connection_index = -1

    def elect_connection(self):
        (last_connection_name, last_connection_index) = \
            (self.__last_connection_name, self.__connection_index)

        connections = list(self.__master.connections)
        connection_count = len(connections)

        self.__connection_index = (self.__connection_index + 1) % \
                                  connection_count

        elected = connections[self.__connection_index]
        elected_name = elected
        self.__last_connection_name = elected_name

        _logger.debug("Elected connection [%s] (%d). COUNT=(%d) "
                      "LAST_NAME=[%s] LAST_INDEX=(%d)", 
                      elected_name, self.__connection_index, connection_count,
                      last_connection_name, last_connection_index)

        return elected.command

    def command_for_all_connections(self, cb):
        """Invoke the callback with a command-object for each connection."""

        for connection in self.__master.connections:
            cb(connection.command)
