/**
[env]
backend = ["surrealkv"]
versioned = true
timeout = 5000

[test]
reason = "A call under a VERSION runs the body stored at that stamp, so the plan-time mutability walk reads the body at the same stamp"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: anchor:0, k: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[{ count: 1 }]"

*/

-- The runtime reads a function's body at the statement's VERSION, so a body
-- redefined since that stamp is not the body that runs. The plan-time walk
-- that decides whether a call may write has to read the same stamp, or it
-- answers about a definition the call will never reach: here the stored body
-- writes and the current one does not.

DEFINE TABLE anchor SCHEMALESS CHANGEFEED 1h;
DEFINE TABLE sink SCHEMALESS CHANGEFEED 1h;
CREATE anchor:0 SET k = 1;
DEFINE FUNCTION fn::f() { CREATE sink:1 SET v = 1; RETURN 1; };
SLEEP 50ms;
LET $t1 = time::now();
SLEEP 50ms;
DEFINE FUNCTION OVERWRITE fn::f() { RETURN 2; };

-- Current state: the read-only body.
SELECT VALUE fn::f() FROM anchor:0;

-- At $t1: the writing body, and its write lands.
SELECT VALUE fn::f() FROM anchor:0 VERSION $t1;

SELECT count() FROM sink GROUP ALL;
