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
        return json.loads(message.body)['type']

c = nsq.consumer.Consumer(_TOPIC, _CHANNEL, nc, _MessageHandler)
c.identify.\
    client_id(11111).\
    heartbeat_interval(10 * 1000)

c.run((_TOPIC, _CHANNEL), 1)
