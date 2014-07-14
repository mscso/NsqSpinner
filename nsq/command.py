import logging
import struct
import io

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

    def __pack(self, data):
        return (struct.pack('!I', len(data)), 
                data)

    def pub(self, topic, message):
        self.__c.send_command(
            ('PUB', topic), 
            self.__pack(message))

    def mpub(self, topic, messages):
        s = io.BytesIO()

        count = 0
        for message in messages:
            for part in self.__pack(message):
                s.write(part)

            count += 1

        multiple_message_data = s.getvalue()

        packed_message_count = struct.pack('!I', count)

        packed_length = struct.pack('!I', 
                            len(packed_message_count) + 
                            len(multiple_message_data))

        self.__c.send_command(
            ('MPUB', topic), 
            [packed_length,
             packed_message_count,
             multiple_message_data])

    def req(self, message_id, timeout_s):
# TODO(dustin): Test this.
# TODO(dustin): Verify that this is actually in seconds.

        self.__c.send_command(
            ('REQ', message_id, timeout_s), 
            wait_for_response=False)

    def touch(self, message_id):
# TODO(dustin): Test this.
        self.__c.send_command(
            ('TOUCH', message_id), 
            wait_for_response=False)

    def cls(self):
        r = self.__c.send_command('CLS')
        _logger.debug("CLS response received ([%s]). Closing connection.", r)

        self.__c.force_quit_ev.set()
