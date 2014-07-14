import logging
import struct
import collections
import json
import pprint
import datetime
import io
import functools

import gevent
import gevent.select
import gevent.queue
import gevent.event
import gevent.ssl

import nsq.config
import nsq.config.protocol
import nsq.config.client
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

        self.__logger.setLevel(logging.INFO)
#
#        if nsq.config.IS_DEBUG is True:
#            self.__logger.setLevel(logging.DEBUG)
#        else:
#            self.__logger.setLevel(logging.INFO)

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
    def __init__(self, node, connection, identify, message_q, nice_quit_ev, 
                 ccallbacks=None, ignore_quit=False):
        self.__node = node
        self.__c = connection
        self.__c_peer = connection.getpeername()

        self.__identify = identify
        self.__message_q = message_q
        self.__ccallbacks = ccallbacks

        self.__command = nsq.command.Command(self)
        self.__last_command = None

        # Receives the response to the last command.
        self.__response_success = None
        self.__response_error = None
        self.__response_ev = gevent.event.Event()

        # The queue for outgoing commands.
        self.__outgoing_q = gevent.queue.Queue()

        self.__buffer = _Buffer()
        self.__read_filters = []
        self.__write_filters = []

        self.__do_buffered_reads = False

        # This event is triggered by the consumer.
        self.__nice_quit_ev = nice_quit_ev
        self.__ignore_quit = ignore_quit

        # This event is triggered by us if the CLS command goes out and gets 
        # responded to.
        self.__force_quit_ev = gevent.event.Event()

    def __str__(self):
        return ('<CONNECTION %s>' % (self.__c_peer,))

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

        # Normally, this doesn't get set until -after- the IDENTIFY response is 
        # processed so that filters/etc can be added/removed before we have 
        # data being buffered. However, since compression requires that data of 
        # one length is received and data of another length is returned, it 
        # makes things a little nicer to just enable it here (a little early), 
        # so that we can grab the Snappy response without difficulty.
        _logger.debug("Enabling buffering a little early (1).")
        self.__do_buffered_reads = True

        # There should be an OK waiting in the pipeline.
        _logger.debug("Waiting for Snappy success response.")
        self.__read_frame()

    def activate_deflate(self, level):
        _logger.info("Activating Deflate compression.")

        import zlib

        wbits = -zlib.MAX_WBITS

        compress = zlib.compressobj(level, zlib.DEFLATED, wbits)

        compressor = lambda x: compress.compress(x) + \
                                compress.flush(zlib.Z_SYNC_FLUSH)

        self.__write_filters.append(compressor)

        decompress = zlib.decompressobj(wbits)
        
        decompressor = lambda x: decompress.decompress(x) + \
                                    decompress.flush()
        
        self.__read_filters.append(decompressor)

        _logger.debug("Enabling buffering a little early (2).")
        self.__do_buffered_reads = True

        # There should be an OK waiting in the pipeline.
        _logger.debug("Waiting for Deflate success response.")
        self.__read_frame()

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

        # There should be an OK waiting in the pipeline.
        _logger.debug("Waiting for SSL success response.")
        self.__read_frame()

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

            # We have to clear this beforehand because we might expect a 
            # couple of signally responses, and they would get routed here if 
            # this is still set.
            self.__last_command = None
            self.__identify.process_response(self, identify_info)

            # We don't want to deal with the complexities of have existing data 
            # in the buffers when we enable SSL or compression.
            self.__do_buffered_reads = True

            if self.__ccallbacks is not None:
                # This has to be spawned, or we might not be able process 
                # responses to any commands that it might send.
                gevent.spawn(self.__ccallbacks.identify, self)

        # Else, store the response. Whoever queued the last command can wait 
        # for the next response to be set. Each connection works in a serial
        # fashion.
        else:
            _logger.debug("Received response (%d bytes) (LAST_COMMAND=[%s]).", 
                          len(data), self.__last_command)

            self.__response_success = data
            self.__response_ev.set()

    def __process_frame_error(self, data):
        if data in nsq.config.protocol.PASSIVE_ERROR_LIST:
            _logger.warning("Passive error received and ignored: [%s]", data)
        else:
            _logger.error("Received ERROR frame: [%s]", data)
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

        # Indicate that this was the last command physically transmitted.
        self.__last_command = command_name

        if issubclass(command.__class__, tuple) is True:
            command = ' '.join([str(part) for part in command])

        self.__primitive_send(command + "\n")
        
        for part in parts:
            self.__primitive_send(part)

    def queue_message(self, command, parts):
        _logger.debug("Queueing command: [%s] (%d)", command, len(parts))

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

            self.__response_ev.wait()

            (response, self.__response_success) = (self.__response_success, None)
            self.__response_ev.clear()

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

    def __filter_incoming_data(self, data):
        # We reverse this because the read and write filters, though added 
        # to their respective lists at the same time, will have to be 
        # processed in the reverse order.
        for f in reversed(self.__read_filters):
            data = f(data)
            if not data:
                break

        return data

    def __read_buffered(self, length):
        while self.__buffer.size < length:
            data = self.__c.recv(nsq.config.client.BUFFER_READ_CHUNK_SIZE_B)
            data = self.__filter_incoming_data(data)
            
            if data:
                self.__buffer.push(data)

        return self.__buffer.read(length)

    def __read_exact(self, length):
        parts = []
        remaining_b = length

        while remaining_b > 0:
            data = self.__c.recv(remaining_b)
            if not data:
                continue

            data_filtered = self.__filter_incoming_data(data)
            filtered_bytes = len(data_filtered)
            data_bytes = len(data)

            if filtered_bytes != data_bytes:
                raise IOError("The amount of bytes returned from filtering "
                              "(%d) -does not- equal the number that went in "
                              "(%d)!" % (filtered_bytes, data_bytes))

            parts.append(data_filtered)
            remaining_b -= filtered_bytes

        return ''.join(parts)

    def __read(self, length):
        if self.__do_buffered_reads is True:
            return self.__read_buffered(length)
        else:
            return self.__read_exact(length)

    def __read_frame(self):
        _logger.debug("Reading frame header.")
        (length,) = struct.unpack('!I', self.__read(4))

        _logger.debug("Reading (%d) more bytes", length)
        (frame_type,) = struct.unpack('!I', self.__read(4))

        data = self.__read(length - 4)

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
                self.__ccallbacks.broken(self)

        gevent.getcurrent().link(terminate_cb)

        if self.__ccallbacks is not None:
            self.__ccallbacks.connect(self)

        # If we're ignoring the quit, the connections will have to be closed 
        # by the server.
        while (self.__ignore_quit is True or \
               self.__nice_quit_ev.is_set() is False) and \
              self.__force_quit_ev.is_set() is False:
            hit = False

# TODO(dustin): Consider breaking the loop if we haven't yet retried to 
#               reconnect a couple of times. A connection will automatically be 
#               reattempted.
            status = gevent.select.select(
                        [self.__c], 
                        [], 
                        [], 
                        timeout=0)

            if status[0]:
                hit = True
                self.__read_frame()

            try:
                (command, parts) = self.__outgoing_q.get(block=False)
            except gevent.queue.Empty:
                pass
            else:
                _logger.debug("Dequeued outgoing command ((%d) remaining): "
                              "[%s]", self.__outgoing_q.qsize(), 
                              self.__distill_command_name(command))

                hit = True
                self.__send_command_primitive(command, parts)

            if hit is False:
                gevent.sleep(nsq.config.client.READWRITE_THROTTLE_S)

        _logger.debug("Connection interaction has stopped (IGNORE_QUIT=[%s] "
                      "NICE_QUIT_EV=[%s] FORCE_QUIT_EV=[%s]): %s", 
                      self.__ignore_quit, self.__nice_quit_ev.is_set(), 
                      self.__force_quit_ev.is_set(), self)

    @property
    def command(self):
        return self.__command

    @property
    def node(self):
        return self.__node

    @property
    def force_quit_ev(self):
        return self.__force_quit_ev


class Connection(object):
    def __init__(self, node, identify, message_q, nice_quit_ev, 
                 ccallbacks=None, ignore_quit=False):
        self.__node = node
        self.__identify = identify
        self.__message_q = message_q
        self.__is_connected = False
        self.__mc = None
        self.__nice_quit_ev = nice_quit_ev
        self.__ccallbacks = ccallbacks
        self.__ignore_quit = ignore_quit

    def __connect(self):
        _logger.debug("Connecting node: [%s]", self.__node)

        c = self.__node.connect(self.__nice_quit_ev)

        _logger.debug("Node connected and being handed-off to be managed: "
                      "[%s]", self.__node)

        self.__is_connected = True

        self.__mc = _ManagedConnection(
                        self.__node,
                        c, 
                        self.__identify,
                        self.__message_q,
                        self.__nice_quit_ev,
                        self.__ccallbacks,
                        ignore_quit=self.__ignore_quit)

        try:
            self.__mc.interact()
        except nsq.exceptions.NsqConnectGiveUpError:
            raise
        finally:
            self.__is_connected = False

    def run(self):
        """Connect the server, and maintain the connection. This shall not 
        return until a connection has been determined to absolutely not be 
        available.
        """

        while self.__nice_quit_ev.is_set() is False:
            self.__connect()

        _logger.info("Connection re-connect loop has terminated: %s", self)

    @property
    def is_connected(self):
        return self.__is_connected

    @property
    def managed_connection(self):
        return self.__mc
