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

# The blocksize of data to read off the wire.
BUFFER_READ_CHUNK_SIZE_B = 8192

# The interval to wait if there isn't any data to read.
READ_THROTTLE_S = .001

# The interval to wait if there aren't any commands to write.
WRITE_THROTTLE_S = .001

# The amount of time to wait between checks that all of the servers are 
# connected, on startup.
CONNECT_AUDIT_WAIT_INTERVAL_S = .1

# We wait for N cycles of GRANULAR_AUDIT_SLEEP_STEP_TIME_S-seconds each before 
# we check for a change in connection state. We do this instead of waiting a 
# contiguous block of time so that we can also check the quit_ev event for 
# whether we've been asked to close the connection rather than having to wait 
# the whole duration.
GRANULAR_CONNECTION_AUDIT_SLEEP_STEP_TIME_S = .5

# The amount of time to wait between checks for all of the connections to have 
# closed.
CONNECTION_CLOSE_AUDIT_WAIT_S = .5

# The amount of time to wait for our connections to close when we're trying to 
# quit before we give up.
CONNECTION_QUIT_CLOSE_TIMEOUT_S = 10
