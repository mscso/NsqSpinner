class ConnectionCallbacks(object):
    def connect(self, connection):
        pass

    def broken(self, connection):
        pass

    def message_received(self, connection, message):
        pass
