This project is in active development.

--------
Features
--------

- Fully featured:

  - Snappy compression
  - DEFLATE compression
  - TLS compression
  - Client ("mutual") authentication via TLS

- IDENTIFY parameters can be specified directly, but many are managed 
  automatically based on parameters to the producer/consumer.

- We rely on the consumer defining a "classification" function to determine the 
  name of a handler for an incoming message. This allows for event-driven 
  consumption. This means a little less boiler-plate for the end-user.

- The complexities of RDY management is automatically managed by the library. 
  These parameters can be reconfigured, but *nsqs* emphasized simplicity and 
  intuitiveness so that you don't have to be involved in mechanics if you don't 
  want to.

- Messages are marked as "finished" with the server after being processed 
  unless we're configured not to.


---------
Footnotes
---------

- Because we rely on `gevent <http://www.gevent.org>`_, and *gevent* isn't 
  Python3 compatible, *nsqs* isn't Python3 compatible.
