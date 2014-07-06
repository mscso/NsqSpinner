#!/usr/bin/env python

import sys
import os.path
dev_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, dev_path)

import logging
import json
import gevent.queue

import nsq.consumer
import nsq.node_collection
import nsq.message_handler
import nsq.connection_callbacks

def _configure_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(format)

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

_configure_logging()

_TOPIC = 'test_topic'
_CHANNEL = 'test_channel'

lookup_node_prefixes = [
    'http://127.0.0.1:4161'
]

_logger = logging.getLogger(__name__)

nc = nsq.node_collection.LookupNodes(lookup_node_prefixes)


class _MessageHandler(nsq.message_handler.MessageHandler):
    def classify_message(self, message):
        decoded = json.loads(message.body)
        return (decoded['type'], decoded)

    def handle_dummy(self, connection, message, context):
        print("Handling!")

    def default_message_handler(self, message_class, connection, message, 
                                classify_context):
        print("Squashing unhandled message: [%s] [%s]" % 
              (message_class, message))


class _ConnectionCallbacks(nsq.connection_callbacks.ConnectionCallbacks):
    def connect(self, c):
        print("Connect!")

    def broken(self, c):
        print("Broken!")

    def message_received(self, connection, message):
        print("Message received!")


# TODO(dustin): We're currently passing the topic and channel twice. The first 
#               is required to derive servers from NSQLOOKUPD hosts (if we're 
#               using them), and the second is required to be able to subscribe 
#               (where the topic/channel is potentially derived fro ma callback 
#               that is given the connection).
#
#               The real question is whether we shouldn't allow our consumer to 
#               represent more than one topic.
c = nsq.consumer.Consumer(
        _TOPIC, 
        _CHANNEL, 
        nc, 
        message_handler_cls=_MessageHandler, 
        tls_ca_bundle_filepath='/Users/dustin/ssl/ca_test/ca.crt.pem',
        tls_auth_pair=('/Users/dustin/ssl/ca_test/client.key.pem', 
                       '/Users/dustin/ssl/ca_test/client.crt.pem'),
        compression=True)

c.identify.\
    client_id('11111').\
    heartbeat_interval(10 * 1000)

c.run((_TOPIC, _CHANNEL), 1, ccallbacks=_ConnectionCallbacks())
