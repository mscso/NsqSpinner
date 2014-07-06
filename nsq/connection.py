import logging
import struct
import collections
import json
import pprint
import datetime
import io

import gevent
import gevent.select
import gevent.queue
import gevent.event
import gevent.ssl

import nsq.config
import nsq.config.protocol
import nsq.constants
import nsq.exceptions
import nsq.node
import nsq.command

TLS_CA_BUNDLE_FILEPATH = None
TLS_AUTH_PAIR = None

_INCOMING_MESSAGE_CLS = collections.namedtuple(
                            'IncomingMessage',
                            ['timestamp_dt',
                             'attempts',
                             'message_id',
                             'body'])

_logger = logging.getLogger(__name__)


class _Buffer(object):
    """A buffer object that retains a stack of whole blocks, and allows us to 
    pop lengths off the head.
    """

    def __init__(self):
        self.__size = 0
        self.__buffers = []
        self.__logger = _logger.getChild('Buffer')

        if nsq.config.IS_DEBUG is True:
            self.__logger.setLevel(logging.DEBUG)
        else:
            self.__logger.setLevel(logging.INFO)

    def push(self, bytes):
        self.__logger.debug("Pushing chunk of (%d) bytes into the buffer. "
                            "TOTAL_BYTES=(%d) TOTAL_CHUNKS=(%d)", 
                            len(bytes), self.__size, len(self.__buffers))

        self.__buffers.append(bytes)
        self.__size += len(bytes)

    def read(self, count):
        self.__logger.debug("Read head-length (%d).", count)

        if count > self.__size:
           raise IOError("Buffers are short: (%d) < (%d)", self.__size, count)

        still_needed_b = count
        clips = []

        while still_needed_b > 0:
            first_buffer_len = len(self.__buffers[0])
            if first_buffer_len < still_needed_b:
                self.__logger.debug("Popping chunk of (%d) bytes off the "
                                    "front of the buffers.", first_buffer_len)

                clips.append(self.__buffers[0])
                del self.__buffers[0]
                still_needed_b -= first_buffer_len
                self.__size -= first_buffer_len

                self.__logger.debug("(%d) chunks remain, of (%d) total bytes.",
                                    len(self.__buffers), self.__size)
            else:
                self.__logger.debug("Splitting (%d) bytes off the front of "
                                    "first buffer of (%d) bytes.", 
                                    still_needed_b, first_buffer_len)

                clips.append(self.__buffers[0][:still_needed_b])
                self.__buffers[0] = self.__buffers[0][still_needed_b:]
                self.__size -= still_needed_b
                still_needed_b = 0

        self.__logger.debug("Joining (%d) segments.", len(clips))
        return ''.join(clips)

    def flush(self):
        """Return all buffered data, and clear the stack."""

        collected = ''.join(self.__buffers)
        self.__buffers = []
        self.__size = 0

        return collected

    @property
    def size(self):
        return self.__size


class _ManagedConnection(object):
    def __init__(self, node, connection, identify, message_q, ccallbacks=None):
        self.__node = node
        self.__c = connection
        self.__identify = identify
        self.__message_q = message_q
        self.__ccallbacks = ccallbacks

        self.__command = nsq.command.Command(self)
        self.__last_command = None

        # Receives the response to the last command.
        self.__response_success = None
        self.__response_error = None
        self.__response_event = gevent.event.Event()

        # The queue for outgoing commands.
        self.__outgoing_q = gevent.queue.Queue()

        self.__buffer = _Buffer()
        self.__read_filters = []
        self.__write_filters = []

    def __str__(self):
        return ('<CONNECTION %s>' % (self.__c.getpeername(),))

    def __send_hello(self):
        """Initiate the handshake."""

        _logger.debug("Saying hello: [%s]", self)

        self.__c.send(nsq.config.protocol.MAGIC_IDENTIFIER)

    def activate_snappy(self):
        _logger.info("Activating Snappy compression.")

        import snappy

        c = snappy.StreamCompressor()
        self.__write_filters.append(c.add_chunk)

        d = snappy.StreamDecompressor()
        self.__read_filters.append(d.decompress)

        existing = self.__buffer.flush()
        decompressed = d.decompress(existing)

        if decompressed:
            self.__buffer.push(decompressed)

    def activate_tlsv1(self):
        if TLS_CA_BUNDLE_FILEPATH is None:
            raise EnvironmentError("We were told to activate TLS, but no CA "
                                   "bundle was set.")

        _logger.info("Activating TLSv1 with [%s]: %s", 
                     TLS_CA_BUNDLE_FILEPATH, self)

        options = {
            'cert_reqs': gevent.ssl.CERT_REQUIRED,
            'ca_certs': TLS_CA_BUNDLE_FILEPATH,
        }

        if TLS_AUTH_PAIR is not None:
            _logger.info("Using TLS authentication: %s", TLS_AUTH_PAIR)
            (options['keyfile'], options['certfile']) = TLS_AUTH_PAIR

        self.__c = gevent.ssl.wrap_socket(
                    self.__c, 
                    ssl_version=gevent.ssl.PROTOCOL_TLSv1, 
                    **options)

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
            identify_info = json.loads(data)
            _logger.debug("Received IDENTIFY response:\n%s", 
                          pprint.pformat(identify_info))

            self.__last_command = None
            self.__identify.process_response(self, identify_info)

        # Else, store the response. Whoever queued the last command can wait 
        # for the next response to be set. Each connection works in a serial
        # fashion.
        else:
            _logger.debug("Received response (%d bytes).", len(data))

            self.__response_success = data
            self.__response_event.set()

    def __process_frame_error(self, data):
        _logger.error("Received ERROR frame: %s", data)

# TODO(dustin): Filter for the couple of non-critical errors, and just display a warning.
        raise nsq.exceptions.NsqErrorResponseError(data)

    def __process_frame_message(self, timestamp_dt, attempts, message_id, 
                                body):
        m = _INCOMING_MESSAGE_CLS(
                timestamp_dt=timestamp_dt, 
                attempts=attempts, 
                message_id=message_id, 
                body=body)

        _logger.debug("Received MESSAGE frame: [%s] (%d bytes)", 
                      message_id, len(body))

        self.__message_q.put((self, m))

    def __primitive_send(self, data):
        for f in self.__write_filters:
            data = f(data)
            if not data:
                break
        
        if data:
            self.__c.send(data)

    def __send_command_primitive(
            self, 
            command, 
            parts):

        command_name = self.__distill_command_name(command)

        _logger.debug("Sending command: [%s] (%d)", 
                      command_name, len(parts))

        self.__last_command = command_name

        if issubclass(command.__class__, tuple) is True:
            command = ' '.join([str(part) for part in command])

        self.__primitive_send(command + "\n")
        
        for part in parts:
            self.__primitive_send(part)

    def queue_message(self, command, parts):
        _logger.debug("Queueing command: [%s] (%d)", 
                      self.__distill_command_name(command), len(parts))

        self.__outgoing_q.put((command, parts))

    def __distill_command_name(self, command):
        """The command may have parameters. Extract the first part."""

        return command[0] if issubclass(command.__class__, tuple) else command

    def send_command(self, command, parts=None, wait_for_response=True):
        if parts is None:
            parts = []

        self.queue_message(command, parts)

        if wait_for_response is True:
            _logger.debug("Waiting for response to [%s].", 
                          self.__distill_command_name(command))

            self.__response_event.wait()

            (response, self.__response_success) = (self.__response_success, None)
            self.__response_event.clear()

            return response
        else:
            _logger.debug("There will be no response.")

    def __process_message(self, frame_type, data):
        if frame_type == nsq.constants.FT_RESPONSE:
            self.__process_frame_response(data)
        elif frame_type == nsq.constants.FT_ERROR:
            self.__process_frame_error(data)
        elif frame_type == nsq.constants.FT_MESSAGE:
            (time_ns, attempts) = struct.unpack('!qH', data[:10])
            message_id = data[10:10 + 16]
            body = data[26:]

            timestamp_dt = datetime.datetime.utcfromtimestamp(time_ns / 1e9)
            self.__process_frame_message(
                timestamp_dt, 
                attempts, 
                message_id, 
                body)
        else:
            _logger.warning("Ignoring frame of invalid type (%d).", frame_type)

    def __read_or_die(self, length):
        while self.__buffer.size < length:
# TODO(dustin): Put the receive block-size into the config.
            data = self.__c.recv(8192)
            
            # We reverse this because the read and write filters, though added 
            # to their respective lists at the same time, will have to be 
            # processed in the reverse order.
            for f in reversed(self.__read_filters):
                data = f(data)
                if not data:
                    break
            
            if data:
                self.__buffer.push(data)

        return self.__buffer.read(length)

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
# TODO(dustin): Something isn't robust enough. If we restart the server a 
#               couple of times, the client won't reconnect.
        def terminate_cb(g):
            # Technically, interact() will never return, so we'll never get here. 
            # But, this is logical.
            self.__is_connected = False

            if self.__ccallbacks is not None:
                gevent.spawn(self.__ccallbacks.broken, self)

        gevent.getcurrent().link(terminate_cb)

        if self.__ccallbacks is not None:
            gevent.spawn(self.__ccallbacks.connect, self)

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
                              "[%s]", self.__outgoing_q.qsize(), 
                              self.__distill_command_name(command))

                self.__send_command_primitive(command, parts)

# TODO(dustin): Move this to config.
            gevent.sleep(.1)

    @property
    def command(self):
        return self.__command

    @property
    def node(self):
        return self.__node


class Connection(object):
    def __init__(self, node, identify, message_q, ccallbacks=None):
        self.__node = node
        self.__identify = identify
        self.__message_q = message_q
        self.__is_connected = False
        self.__mc = None
        self.__ccallbacks = ccallbacks

    def __connect(self):
        _logger.debug("Connecting node: [%s]", self.__node)

        c = self.__node.connect()

        _logger.debug("Node connected and being handed-off to be managed: "
                      "[%s]", self.__node)

        self.__is_connected = True

        self.__mc = _ManagedConnection(
                        self.__node,
                        c, 
                        self.__identify,
                        self.__message_q,
                        self.__ccallbacks)

        try:
            self.__mc.interact()
        except nsq.exceptions.NsqConnectGiveUpError:
            raise

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

    @property
    def managed_connection(self):
        return self.__mc
