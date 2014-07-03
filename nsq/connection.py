import logging
import struct

import gevent
import gevent.select

import nsq.constants
import nsq.config.protocol
import nsq.node
import nsq.command

_logger = logging.getLogger(__name__)


class _ManagedConnection(object):
    def __init__(self, connection, topic, channel):
        self.__c = connection
        self.__topic = topic
        self.__channel = channel

        self.__command = nsq.command.Command(self.__c)

    def __say_hello(self):
        """Initiate the handshake."""

        _logger.debug("Saying hello: [%s]", self.__c)

        self.__c.send(nsq.config.protocol.MAGIC_IDENTIFIER)

    def __identify(self):
        """Send some [optional] connection parameters."""

        pass

    def __process_frame_response(self, data):
        _logger.debug("Received RESPONSE frame:\n%s", data)

        if data == nsq.config.protocol.HEARTBEAT_RESPONSE:
            _logger.debug("Responding to heartbeat.")
            self.__command.nop()

# TODO(dustin): Finish.

    def __process_frame_error(self, data):
        _logger.debug("Received ERROR frame:\n%s", data)
# TODO(dustin): Finish.

    def __process_frame_message(self, time_ns, attempts, message_id, body):
        _logger.debug("Received MESSAGE frame: [%d]:[%d]:[%s]\n%s", 
                      time_ns, attempts, message_id)

        _logger.debug("Message body:\n%s", body)

# TODO(dustin): Finish.

    def __process_message(self, frame_type, data):
        if frame_type == nsq.constants.FT_RESPONSE:
            self.__process_frame_response(data)
        elif frame_type == nsq.constants.FT_ERROR:
            self.__process_frame_error(data)
        elif frame_type == nsq.constants.FT_MESSAGE:
            (time_ns, attempts) = struct.unpack('!qH', data[:10])
            message_id = data[10:10 + 16]
            body = data[26:]

            self.__process_frame_message(time_ns, attempts, message_id, body)
        else:
            _logger.warning("Ignoring frame of invalid type (%d).", frame_type)

    def __read_or_die(self, length):
        data = self.__c.recv(length)
        len_ = len(data)
        if len_ != length:
            raise IOError("Could not read (%d) bytes from socket: (%d) != "
                          "(%d)" % (length, len_, length))

        return data

    def __process_frame(self):
        (length, frame_type) = struct.unpack('!II', self.__read_or_die(8))
        data = self.__read_or_die(length)

        self.__process_message(frame_type, data)

    def interact(self):
        _logger.info("Now engaged: [%s]", self.__c)

        self.__say_hello()
        self.__identify()

        while 1:
            status = gevent.select.select(
                        [self.__c], 
                        [], 
                        [], 
                        timeout=nsq.config.protocol.READ_SELECT_TIMEOUT_S)

            if status[0]:
                self.__process_frame()

# TODO(dustin): Also send queued messages.
# TODO(dustin): We need to interact a read and write queue that our user can 
#               push and pop messages into.

#            gevent.sleep(1)


class Connection(object):
    def __init__(self, node, topic, channel):
        self.__node = node
        self.__topic = topic
        self.__channel = channel

    def __connect(self):
        c = self.__node.connect()

        try:
            _ManagedConnection(c, self.__topic, self.__channel).interact()
        except nsq.node.NsqConnectGiveUpError:
            raise

    def run(self):
        """Connect the server, and maintain the connection. This shall not 
        return until a connection has been determined to absolutely not be 
        available.
        """

        while 1:
            _logger.debug("Connecting to: [%s]", self.__node)
            self.__connect()
