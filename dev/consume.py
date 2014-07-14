#!/usr/bin/env python

import sys
import os.path
dev_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, dev_path)

import logging
import json
import gevent
import time

import nsq.consumer
import nsq.node_collection
import nsq.message_handler
import nsq.identify

def _configure_logging():
    logging.getLogger('requests').setLevel(logging.WARNING)

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

_logger = logging.getLogger(__name__)

lookup_node_prefixes = [
    'http://127.0.0.1:4161',
]

nc = nsq.node_collection.LookupNodes(lookup_node_prefixes)

#server_nodes = [
#    ('127.0.0.1', 4150),
#]
#
#nc = nsq.node_collection.ServerNodes(server_nodes)


class _MessageHandler(nsq.message_handler.MessageHandler):
    def __init__(self, *args, **kwargs):
        super(_MessageHandler, self).__init__(*args, **kwargs)
        self.__processed = 0

    def message_received(self, connection, message):
        super(_MessageHandler, self).message_received(connection, message)

        try:
            self.__decoded = json.loads(message.body)
        except:
            _logger.info("Couldn't decode message. Finished: [%s]", 
                         message.body)
            return

    def classify_message(self, message):
        return (self.__decoded['type'], self.__decoded)

    def handle_dummy(self, connection, message, context):
        self.__processed += 1

        if self.__processed % 1000 == 0:
            _logger.info("Processed (%d) messages.", self.__processed)

    def default_message_handler(self, message_class, connection, message, 
                                classify_context):
        _logger.warning("Squashing unhandled message: [%s] [%s]",
                        message_class, message)


#i = nsq.identify.Identify()
#i.\
#    heartbeat_interval(10 * 1000)
##    client_id('11111').\

# TODO(dustin): If we connect while there are already jobs waiting to be 
#               handled, we'll receive one, and then have to wait thirty-
#               seconds until we receive the rest.

# TODO(dustin): This should return an object that we can use to control it.
c = nsq.consumer.Consumer(
        _TOPIC, 
        _CHANNEL, 
        nc, 
        500, 
        message_handler_cls=_MessageHandler, 
#        tls_ca_bundle_filepath='/Users/dustin/ssl/ca_test/ca.crt.pem',
#        tls_auth_pair=('/Users/dustin/ssl/ca_test/client.key.pem', 
#                       '/Users/dustin/ssl/ca_test/client.crt.pem'),
#        compression='deflate',
        compression=True)#,
#        identify=i)

c.start()

#n = 20
#while n > 0:
#    gevent.sleep(1)
#
#    n -= 1

while c.is_alive:
    gevent.sleep(1)

if c.is_alive:
    _logger.info("Stopping consumer.")
    c.stop()
