import logging
import json
import struct

_logger = logging.getLogger(__name__)

IDENTIFY_COMMAND = 'IDENTIFY'


class Identify(object):
    def __init__(self):
        self.__parameters = {}
        self.__cached = None
        self.__server_features = None

    def update(self, parameters):
        self.__cached = None
        self.__parameters.update(parameters)

    def enqueue(self, connection):
        if not self.__parameters:
            _logger.warning("No IDENTIFY parameters defined. Skipping.")
            return

        if self.__cached is None:
            self.__cached = json.dumps(self.__parameters)
            _logger.debug("Using IDENTIFY body:\n%s", self.__cached)

        len_ = len(self.__cached)

        _logger.debug("Sending IDENTIFY: (%d) %s", len_, self.__cached)

        connection.queue_message(
                    'IDENTIFY',
                    [struct.pack('!I', len_),
                     self.__cached])

    def process_response(self, connection, identify_info):

# TODO(dustin): Does a producer receive a job response, or do we have to push a 
#               new job in order to deliver the result?

        try:
            if identify_info['tls_v1'] is True:
                connection.activate_tlsv1()
        except KeyError:
            pass

        try:
            if identify_info['snappy'] is True:
                connection.activate_snappy()
        except KeyError:
            pass

        # We're going to get a copy of this for each connection, but they 
        # should all be identical.
        self.__server_features = identify_info

    def __push(self, k, v):
        self.__parameters[k] = v

        return self

    def __str__(self):
        if self.__parameters:
            parameter_phrase = ' '.join(
                [('%s=[%s]' % (k, v)) 
                 for (k, v) 
                 in self.__parameters.items()])
        else:
            parameter_phrase = '(empty)'

        return '<IDENTIFY %s>' % (parameter_phrase,)

    def __getitem__(self, key):
        return self.__parameters[key]

    def get(self, key):
        return self.__parameters.get(key)

    def client_id(self, client_id):
        """client_id an identifier used to disambiguate this client (ie. 
        something specific to the consumer)
        """

        return self.__push('client_id', client_id)

    def hostname(self, hostname):
        """hostname the hostname where the client is deployed"""

        return self.__push('hostname', hostname)

    def set_feature_negotiation(self):
        """feature_negotiation (nsqd 0.2.19+) bool used to indicate that the 
        client supports feature negotiation. If the server is capable, it will 
        send back a JSON payload of supported features and metadata.
        """

        return self.__push('feature_negotiation', True)

    def heartbeat_interval(self, heartbeat_interval_ms):
        """heartbeat_interval (nsqd 0.2.19+) milliseconds between heartbeats.

        Valid range: 1000 <= heartbeat_interval <= configured_max (-1 disables 
        heartbeats)

        --max-heartbeat-interval (nsqd flag) controls the max

        Defaults to --client-timeout / 2
        """

        assert issubclass(heartbeat_interval_ms.__class__, int)

        return self.__push('heartbeat_interval', heartbeat_interval_ms)

    def output_buffer_size(self, output_buffer_size_b):
        """output_buffer_size (nsqd 0.2.21+) the size in bytes of the buffer 
        nsqd will use when writing to this client.

        Valid range: 64 <= output_buffer_size <= configured_max (-1 disables 
        output buffering)

        --max-output-buffer-size (nsqd flag) controls the max

        Defaults to 16kb
        """

        assert issubclass(output_buffer_size_b.__class__, int)

        return self.__push('output_buffer_size', output_buffer_size_b)

    def output_buffer_timeout(self, output_buffer_timeout_ms):
        """output_buffer_timeout (nsqd 0.2.21+) the timeout after which any 
        data that nsqd has buffered will be flushed to this client.

        Valid range: 1ms <= output_buffer_timeout <= configured_max (-1 
        disables timeouts)

        --max-output-buffer-timeout (nsqd flag) controls the max

        Defaults to 250ms

        Warning: configuring clients with an extremely low (< 25ms) 
        output_buffer_timeout has a significant effect on nsqd CPU usage 
        (particularly with > 50 clients connected).

        This is due to the current implementation relying on Go timers which 
        are maintained by the Go runtime in a priority queue. See the commit 
        message in pull request #236 for more details.
        """

        return self.__push('output_buffer_timeout', output_buffer_timeout_ms)

    def set_tls_v1(self):
        """tls_v1 (nsqd 0.2.22+) enable TLS for this connection.

        --tls-cert and --tls-key (nsqd flags) enable TLS and configure the 
        server certificate

        If the server supports TLS it will reply "tls_v1": true

        The client should begin the TLS handshake immediately after reading the 
        IDENTIFY response

        The server will respond OK after completing the TLS handshake
        """

        return self.__push('tls_v1', True)

    def set_snappy(self):
        """snappy (nsqd 0.2.23+) enable snappy compression for this connection.

        --snappy (nsqd flag) enables support for this server side

        The client should expect an additional, snappy compressed OK response 
        immediately after the IDENTIFY response.

        A client cannot enable both snappy and deflate.
        """

        return self.__push('snappy', True)

    def set_deflate(self):
        """deflate (nsqd 0.2.23+) enable deflate compression for this connection.

        --deflate (nsqd flag) enables support for this server side

        The client should expect an additional, deflate compressed OK response immediately after the IDENTIFY response.

        A client cannot enable both snappy and deflate.
        """

        return self.__push('deflate', True)

    def deflate_level(self, deflate_level):
        """deflate_level (nsqd 0.2.23+) configure the deflate compression level 
        for this connection.

        --max-deflate-level (nsqd flag) configures the maximum allowed value

        Valid range: 1 <= deflate_level <= configured_max

        Higher values mean better compression but more CPU usage for nsqd.
        """

        assert issubclass(deflate_level.__class__, int)

        return self.__push('deflate_level', deflate_level)

    def sample_rate(self, sample_rate):
        """sample_rate (nsqd 0.2.25+) sample messages delivered over this 
        connection.

        Valid range: 0 <= sample_rate <= 99 (0 disables sampling)

        Defaults to 0
        """

        assert issubclass(sample_rate.__class__, int)

        return self.__push('sample_rate', sample_rate)

    def user_agent(self, user_agent):
        """user_agent (nsqd 0.2.25+) a string identifying the agent for this 
        client in the spirit of HTTP

        Default: <client_library_name>/<version>
        """

        return self.__push('user_agent', user_agent)

    def msg_timeout(self, msg_timeout_ms):
        """msg_timeout (nsqd 0.2.28+) configure the server-side message timeout 
        for messages delivered to this client.
        """

        assert issubclass(msg_timeout_ms.__class__, int)

        return self.__push('msg_timeout', msg_timeout_ms)

    @property
    def parameters(self):
        return self.__parameters

    @property
    def server_features(self):
        return self.__server_features
