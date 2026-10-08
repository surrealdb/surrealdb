/**
[env]

[test]
reason = "Test EXPLAIN with SLEEP statement"

[[test.results]]
value = '''
'Sleep [ctx: Rt] [duration: 100ms]
''''

[[test.results]]
value = '''
'Sleep [ctx: Rt] [duration: 1s]
''''

[[test.results]]
value = '''
'Sleep [ctx: Rt] [duration: 500µs]
''''

*/

-- EXPLAIN SLEEP with milliseconds
EXPLAIN SLEEP 100ms;

-- EXPLAIN SLEEP with seconds
EXPLAIN SLEEP 1s;

-- EXPLAIN SLEEP with microseconds
EXPLAIN SLEEP 500us;
