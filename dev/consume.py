#!/usr/bin/env python

import sys
import os.path
dev_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, dev_path)

import logging
import json

import nsq.consumer
import nsq.node_collection
import nsq.message_handler
import nsq.identify

def _configure_logging():
    logger = logging.getLogger()
#    logger.setLevel(logging.DEBUG)
    logger.setLevel(logging.INFO)

    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(format)

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

_configure_logging()

_TOPIC = 'test_topic'
_CHANNEL = 'test_channel'

# TODO(dustin): Test using direct TCP endpoints (not using lookupd)

lookup_node_prefixes = [
    'http://127.0.0.1:4161'
]

_logger = logging.getLogger(__name__)

nc = nsq.node_collection.LookupNodes(lookup_node_prefixes)


class _MessageHandler(nsq.message_handler.MessageHandler):
    def message_received(self, connection, message):
        super(_MessageHandler, self).message_received(connection, message)
        self.__decoded = json.loads(message.body)

    def classify_message(self, message):
        return (self.__decoded['type'], self.__decoded)

    def handle_dummy(self, connection, message, context):
        print("Handling: %s" % (self.__decoded,))

    def default_message_handler(self, message_class, connection, message, 
                                classify_context):
        print("Squashing unhandled message: [%s] [%s]" % 
              (message_class, message))


i = nsq.identify.Identify()
i.\
    heartbeat_interval(10 * 1000)
#    client_id('11111').\

# TODO(dustin): If we connect while there are already jobs waiting to be 
#               handled, we'll receive one, and then have to wait thirty-
#               seconds until we receive the rest.

nsq.consumer.consume(
    _TOPIC, 
    _CHANNEL, 
    nc, 
    500, 
    message_handler_cls=_MessageHandler, 
#    tls_ca_bundle_filepath='/Users/dustin/ssl/ca_test/ca.crt.pem',
#    tls_auth_pair=('/Users/dustin/ssl/ca_test/client.key.pem', 
#                   '/Users/dustin/ssl/ca_test/client.crt.pem'),
#    compression='deflate',
    compression=True,
    identify=i)

c.run()
