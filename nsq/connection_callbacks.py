class ConnectionCallbacks(object):
    def connect(self, connection):
        """The connection has been established."""

        pass

    def identify(self, connection):
        """The identify response has been processed for this connection."""

        pass

    def broken(self, connection):
        """The connection has been broken."""

        pass

    def message_received(self, connection, message):
        """A message has been received."""

        pass
