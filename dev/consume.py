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
import nsq.identify

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


i = nsq.identify.Identify()
i.\
    client_id('11111').\
    heartbeat_interval(10 * 1000)

nsq.consumer.consume(
    _TOPIC, 
    _CHANNEL, 
    nc, 
    _ConnectionCallbacks(),
    500, 
    message_handler_cls=_MessageHandler, 
    tls_ca_bundle_filepath='/Users/dustin/ssl/ca_test/ca.crt.pem',
#    tls_auth_pair=('/Users/dustin/ssl/ca_test/client.key.pem', 
#                   '/Users/dustin/ssl/ca_test/client.crt.pem'),
    compression=True,
    identify=i)

c.run()
