class MessageHandler(object):
    """The base-class of the handler for all incoming messages."""

    def handle_incoming(self, message_q):
        raise NotImplementedError()
