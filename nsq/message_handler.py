import logging
import functools

import nsq.config.handle

_logger = logging.getLogger(__name__)

# TODO(dustin): We like having exceptions at the top of module. Eliminate 
#               "exceptions" module.


class MessageHandleException(Exception):
    pass


class MessageUnhandledError(Exception):
    pass


class MessageManuallyFinishedException(MessageHandleException):
    pass

# TODO(dustin): The starvation provisions spelled-out under "Message Flow 
#               Starvation" at:
#
#                   http://nsq.io/clients/building_client_libraries.html#rdy_state 
#
#               isn't relevant, if we read it right. Our aggregate RDY count 
#               always equals max_in_flight (which won't have the problems 
#               associated with an "even distribution of RDY count")


class MessageHandler(object):
    """The base-class of the handler for all incoming messages."""

    def __init__(self, connection_election, ccallbacks=None):
        self.__ce = connection_election
        self.__ccallbacks = ccallbacks

    def run(self, message_q):
        _logger.debug("Message-handler waiting for messages.")

        def finish(message):
# TODO(dustin): We'll use election whenever we need to initiate a connection-
#               agnostic command.
            self.__ce.elect_connection().fin(message.message_id)

        while 1:
            (connection, message) = message_q.get()

            self.message_received(connection, message)

            try:
                (message_class, classify_context) = \
                    self.classify_message(message)
            except:
                if nsq.config.handle.FINISH_IF_CLASSIFY_ERROR is True:
                    _logger.exception("Could not classify message [%s]. We're "
                                      "configured to just mark it as "
                                      "finished.", message.message_id)

                    finish(message)
                else:
                    _logger.exception("Could not classify message [%s]. We're "
                                      "not configured to 'finish' it, so "
                                      "we'll ignore it.", message.message_id)

                continue

            _logger.debug("Message classified: [%s]", message_class)

            handle = getattr(self, 'handle_' + message_class, None)

            if handle is None:
                handle = functools.partial(
                            self.default_message_handler, 
                            message_class)

            try:
                handle(connection, message, classify_context)
            except MessageManuallyFinishedException:
                pass
            else:
                finish(message)

            if nsq.config.handle.AUTO_FINISH_MESSAGES is True:
                self.message_handled(message)

    def default_message_handler(self, message_class, connection, message, 
                                classify_context):
        """This receives all messages that haven't been otherwise handled. This 
        makes it easy if all you need to handle is one type of message, and 
        don't or can't impose a type into the message body (for 
        classification).
        """

        raise MessageUnhandledError(message_class)

    def message_handled(self, message):
        """A tick function to allow the implementation to do something after 
        every handled message.
        """

        pass

    def message_received(self, connection, message):
        """Just a tick function to allow the implementation to do things as any
        message is received.
        """

        self.__ccallbacks.message_received(connection, message)

    @property
    def ce(self):
        """Dispatch a command."""

        return self.__ce

    def classify_message(self, message):
        raise NotImplementedError()
