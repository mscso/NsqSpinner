import logging

_logger = logging.getLogger(__name__)


class Command(object):
    def __init__(self, connection):
        self.__c = connection

    def nop(self):
        _logger.debug("Sending NOP.")
        self.__c.send("NOP\n")
