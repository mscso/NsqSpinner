import logging
import struct
import collections

import gevent
import gevent.select
import gevent.queue
import gevent.event

import nsq.constants
import nsq.exceptions
import nsq.config.protocol
import nsq.node
import nsq.command

_INCOMING_MESSAGE_CLS = collections.namedtuple(
                            'IncomingMessage',
                            ['time_ns',
                             'attempts',
                             'message_id',
                             'body'])

_logger = logging.getLogger(__name__)


class _ManagedConnection(object):
    def __init__(self, connection, identify, message_q):
        self.__c = connection
        self.__identify = identify
        self.__message_q = message_q

        self.__command = nsq.command.Command(self)
        self.__last_command = None

        # Receives the response to the last command.
        self.__response_success = None
        self.__response_error = None
        self.__response_event = gevent.event.Event()

        # Receives incoming jobs.
        self.__message_q = gevent.queue.Queue()

        # The queue for outgoing commands.
        self.__outgoing_q = gevent.queue.Queue()

    def __send_hello(self):
        """Initiate the handshake."""

        _logger.debug("Saying hello: [%s]", self.__c)

        self.__c.send(nsq.config.protocol.MAGIC_IDENTIFIER)

    def __process_frame_response(self, data):
        # Heartbeats, which arrive as responses, won't interfere with the
        # responses to commands since we'll handle it right here.
        if data == nsq.config.protocol.HEARTBEAT_RESPONSE:
            _logger.debug("Responding to heartbeat.")

            self.__last_command = None
            self.__command.nop()

        # The very first command is an IDENTIFY, so this is the very first true
        # response.
        elif self.__last_command == nsq.identify.IDENTIFY_COMMAND:
            _logger.debug("Received IDENTIFY response:\n%s", data)

            self.__last_command = None
            self.__identify.process_response(data)

        # Else, store the response. Whoever queued the last command can wait 
        # for the next response to be set. Each connection works in a serial
        # fashion.
        else:
            _logger.debug("Received response (%d bytes).", len(data))

            self.__response_success = data
            self.__response_event.set()

    def __process_frame_error(self, data):
        _logger.error("Received ERROR frame: %s", data)
        self.__response_error = data
        self.__response_event.set()

    def __process_frame_message(self, time_ns, attempts, message_id, body):
        m = _INCOMING_MESSAGE_CLS(
                time_ns=time_ns, 
                attempts=attempts, 
                message_id=message_id, 
                body=body)

        _logger.debug("Received MESSAGE frame: [%s] (%d bytes)", 
                      message_id, len(body))

        self.__message_q.add(m)

    def __send_command_primitive(
            self, 
            command, 
            parts):
        _logger.debug("Sending command: [%s] (%d)", 
                      command, len(parts))

        self.__last_command = command

        self.__c.send(command + "\n")
        
        for part in parts:
            self.__c.send(part)

    def queue_message(self, command, parts):
        _logger.debug("Queueing command: [%s] (%d)", 
                      command, len(parts))

        self.__outgoing_q.put((command, parts))

    def send_command(self, command, parts=None, wait_for_response=True):
        if parts is None:
            parts = []

        self.queue_message(command, parts)

        if wait_for_response is True:
            _logger.debug("Waiting for response to [%s].", command)
            self.__response_event.wait()

            if self.__response_error is not None:
                (response_error, self.__response_error) = (self.__response_error, None)
                self.__response_event.clear()

                raise nsq.exceptions.NsqErrorResponseError(response_error)

            elif self.__response_success is not None:
                (response, self.__response_success) = (self.__response_success, None)
                self.__response_event.clear()

            else:
                raise ValueError("Could not determine the response for this "
                                 "message.")

            return response

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
            print(data)
            raise IOError("Could not read (%d) bytes from socket: (%d) != "
                          "(%d)" % (length, len_, length))

        return data

    def __read_frame(self):
        _logger.debug("Reading frame header.")
        (length,) = struct.unpack('!I', self.__read_or_die(4))

        _logger.debug("Reading (%d) more bytes", length)

        (frame_type,) = struct.unpack('!I', self.__read_or_die(4))
        data = self.__read_or_die(length - 4)

        self.__process_message(frame_type, data)

    def interact(self):
        _logger.info("Now engaged: [%s]", self.__c)

        self.__send_hello()
        self.__identify.enqueue(self)

        while 1:
# TODO(dustin): Consider breaking the loop if we haven't yet retried to 
#               reconnect a couple of times. A connection will automatically be 
#               reattempted.
            status = gevent.select.select(
                        [self.__c], 
                        [], 
                        [], 
                        timeout=0)#nsq.config.protocol.READ_SELECT_TIMEOUT_S)

            if status[0]:
                self.__read_frame()

            try:
                (command, parts) = self.__outgoing_q.get(block=False)
            except gevent.queue.Empty:
                pass
            else:
                _logger.debug("Dequeued outgoing command ((%d) remaining): "
                              "[%s]", self.__outgoing_q.qsize(), command)

                self.__send_command_primitive(command, parts)

# TODO(dustin): Move this to config.
            gevent.sleep(.1)


class Connection(object):
    def __init__(self, node, identify, message_q):
        self.__node = node
        self.__identify = identify
        self.__message_q = message_q
        self.__is_connected = False

    def __connect(self):
        _logger.debug("Connecting node: [%s]", self.__node)
        c = self.__node.connect()

        _logger.debug("Node connected and being handed-off to be managed: "
                      "[%s]", self.__node)

        self.__is_connected = True

        mc = _ManagedConnection(
                c, 
                self.__identify,
                self.__message_q)

        try:
            mc.interact()
        except nsq.exceptions.NsqConnectGiveUpError:
            raise

        # Technically, interact() will never return, so we'll never get here. 
        # But, this is logical.
        self.__is_connected = False

    def run(self):
        """Connect the server, and maintain the connection. This shall not 
        return until a connection has been determined to absolutely not be 
        available.
        """

        while 1:
            self.__connect()

    @property
    def is_connected(self):
        return self.__is_connected
