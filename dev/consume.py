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
    def message_received(self, connection, message):
# TODO(dustin): We need to re-set our RDY when we decrement to zero... We'll 
#               have to track our RDY's for every connection, and intelligently 
#               choose values.
        pass

    def classify_message(self, message):
        decoded = json.loads(message.body)
        return (decoded['type'], decoded)

    def handle_dummy(self, connection, message, context):
        print("Handling!")

c = nsq.consumer.Consumer(_TOPIC, _CHANNEL, nc, _MessageHandler)
c.identify.\
    client_id(11111).\
    heartbeat_interval(10 * 1000)


class _ConnectionCallbacks(nsq.connection_callbacks.ConnectionCallbacks):
    def connect(self, c):
        print("Connect!")

    def broken(self, c):
        print("Broken!")

c.run((_TOPIC, _CHANNEL), 1, ccallbacks=_ConnectionCallbacks())
