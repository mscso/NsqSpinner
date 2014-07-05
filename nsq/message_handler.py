import logging

import nsq.config.handle

_logger = logging.getLogger(__name__)


class MessageHandler(object):
    """The base-class of the handler for all incoming messages."""

    def __init__(self, connection_election):
        self.__ce = connection_election

    def run(self, message_q):
        _logger.debug("Message-handler waiting for messages.")

        while 1:
            message = message_q.get()

            try:
                message_class = self.classify_message(message)
            except:
                if nsq.config.handle.FINISH_IF_CLASSIFY_ERROR is True:
                    _logger.warn("Could not classify message [%s]. We're "
                                 "configured to just mark it as finished.",
                                 message.message_id)

                    self.__ce.elect_connection().fin(message.message_id)
                else:
                    _logger.error("Could not classify message [%s]. We're not "
                                  "configured to 'finish' it, so we'll ignore "
                                  "it.", message.message_id)

                continue

            getattr(self, 'handle_' + message_class)(message)

    def classify_message(self, message):
        raise NotImplementedError()
