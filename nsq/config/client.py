import random

# How often to re-poll the lookup servers (if relevant).
LOOKUP_READ_INTERVAL_S = 60

# How often to evaluate the state of our connections.
CONNECTION_AUDIT_WAIT_S = 2

# How long to initially wait before reattempting a connection.
INITIAL_CONNECT_FAIL_WAIT_S = 5

# How much we'll increase the wait for each subsequent failure, plus a random 
# factor to mitigate a thundering herd.
CONNECT_FAIL_WAIT_BACKOFF_RATE = 2 * (1 + random.random())

# The maximum amount of time that we'll wait between connection attempts.
MAXIMUM_CONNECT_FAIL_WAIT_S = 120

# The maximum amount of elapsed time to attempt to connect.
MAXIMUM_CONNECT_ATTEMPT_PERIOD_S = 60 * 5
