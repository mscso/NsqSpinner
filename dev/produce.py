#!/usr/bin/env python

import sys
import os.path
dev_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, dev_path)

import logging
import json
import random

import nsq.producer
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

server_nodes = [
    ('127.0.0.1', 4150),
]

_logger = logging.getLogger(__name__)

nc = nsq.node_collection.ConsumerNodes(server_nodes)


#i = nsq.identify.Identify()
#i.\
#    client_id('11111').\
#    heartbeat_interval(10 * 1000)

p = nsq.producer.Producer(
        _TOPIC, 
        nc, 
#        tls_ca_bundle_filepath='/Users/dustin/ssl/ca_test/ca.crt.pem',
#        tls_auth_pair=('/Users/dustin/ssl/ca_test/client.key.pem', 
#                       '/Users/dustin/ssl/ca_test/client.crt.pem'),
        compression=True)#,
#        identify=i)

p.run()

for i in range(1000):
    if i % 50 == 0:
        print(i)

    data = { 'type': 'dummy', 'data': random.random(), 'index': i }
    message = json.dumps(data)
    p.publish(message)

p.finish_and_quit()
