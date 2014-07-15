This project is in active development, and the documentation is evolving as 
individual pieces.


--------
Features
--------

- Fully featured:

  - Snappy compression
  - DEFLATE compression
  - TLS compression
  - Client ("mutual") authentication via TLS

- We rely on the consumer defining a "classification" function to determine the 
  name of a handler for an incoming message. This allows for event-driven 
  consumption. This means a little less boiler-plate for the end-user.

- The complexities of RDY management is automatically managed by the library. 
  These parameters can be reconfigured, but *nsqs* emphasized simplicity and 
  intuitiveness so that you don't have to be involved in mechanics if you don't 
  want to.

- IDENTIFY parameters can be specified directly, but many are managed 
  automatically based on parameters to the producer/consumer.

- Messages are marked as "finished" with the server after being processed 
  unless we're configured not to.


-----------------------
Implementing a Consumer
-----------------------

Imports and boilerplate::

    import logging
    import json
    import gevent

    import nsq.consumer
    import nsq.node_collection
    import nsq.message_handler

    _logger = logging.getLogger(__name__)

Create a message-handler::

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

Define the node-collection. We use *nsqlookupd* servers here, but we could just 
as easily use `ServerNodes()` with *nsqd* servers::

    lookup_node_prefixes = [
        'http://127.0.0.1:4161',
    ]

    nc = nsq.node_collection.LookupNodes(lookup_node_prefixes)

Create the consumer object::

    _TOPIC = 'test_topic'
    _CHANNEL = 'test_channel'
    _MAX_IN_FLIGHT = 500

    c = nsq.consumer.Consumer(
            _TOPIC, 
            _CHANNEL, 
            nc, 
            _MAX_IN_FLIGHT, 
            message_handler_cls=_MessageHandler)

Start the consumer::

    c.start()

Loop. As an example, we loop as long as we're connected to at least one 
server::

    while c.is_alive:
        gevent.sleep(1)


-----------------------
Implementing a Producer
-----------------------

Imports and boilerplate::

    import logging
    import json
    import random

    import nsq.producer
    import nsq.node_collection
    import nsq.message_handler

    _logger = logging.getLogger(__name__)

Define the node-collection. This is a producer, so it only works with *nsqd* 
nodes::

    server_nodes = [
        ('127.0.0.1', 4150),
    ]

    nc = nsq.node_collection.ServerNodes(server_nodes)

Create the producer object::

    _TOPIC = 'test_topic'

    p = nsq.producer.Producer(_TOPIC, nc)

Start the producer::

    p.start()

Emit the messages::

    for i in range(0, 100000, 10):
        if i % 50 == 0:
            _logger.info("(%d) messages published.", i)

        data = { 'type': 'dummy', 'data': random.random(), 'index': i }
        message = json.dumps(data)
        p.mpublish((message,) * 10)

Stop the producer::

    p.stop()


---------
Callbacks
---------

Both the consumer and producer can take a callbacks object.

To instantiate the callbacks for a *producer*::

    import nsq.connection_callbacks
    cc = nsq.connection_callbacks.ConnectionCallbacks()

To instantiate the callbacks for a *consumer*::

    import nsq.consumer
    cc = nsq.consumer.ConsumerCallbacks()

Then, pass the object into the producer or consumer object constructors as 
`ccallbacks`.

The following callback methods can be implemented for both a producer or 
consumer (while making sure to call the original implementation):

- `connect(connection)`

  The connection has been established.

- `identify(connection)`

  The identify response has been processed for this connection.

- `broken(connection)`

  The connection has been broken.

- `message_received(connection, message)`

  A message has been received.

The *consumer* has one additional callback:

- `rdy_replenish(connection, current_rdy, original_rdy)`

  The RDY needs to be updated. By default, the original RDY will be reemitted. 
  If this is not desired, override this callback, and don't call the original.


---------
Footnotes
---------

- Because we rely on `gevent <http://www.gevent.org>`_, and *gevent* isn't 
  Python3 compatible, *nsqs* isn't Python3 compatible.
