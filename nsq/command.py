import logging

_logger = logging.getLogger(__name__)


class Command(object):
    def __init__(self, connection):
        self.__c = connection

    def nop(self):
        _logger.debug("Sending NOP.")
        self.__c.send_command('NOP', wait_for_response=False)

    def rdy(self, count):
        _logger.debug("Sending RDY of (%d).", count)

        self.__c.send_command(('RDY', count), wait_for_response=False)

    def sub(self, topic, channel):
        self.__c.send_command(('SUB', topic, channel))

    def fin(self, message_id):
        self.__c.send_command(('FIN', message_id), wait_for_response=False)
