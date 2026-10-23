/**
[env]
backend = ["surrealkv"]
versioned = true
timeout = 5000

[test]
reason = """
	A user-defined function called from a statement under `VERSION` is the
	definition stored at the stamp, whichever statement holds the call: the
	versioned statement itself, a subquery of its projection, or the body of
	another function it calls. The body's own reads are at the stamp too.

	What a versioned statement lowers a call to is its own: the statement
	after it, at the current definitions, calls the function that is stored
	now.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: anchor:0, k: 1 }]"

[[test.results]]
value = "[{ id: reading:1, v: 'old' }]"

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
value = "[{ id: reading:1, v: 'new' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['old']"

[[test.results]]
value = "[['old']]"

[[test.results]]
value = "['redefined']"

[[test.results]]
value = "['old']"
*/

DEFINE TABLE anchor SCHEMALESS;
DEFINE TABLE reading SCHEMALESS;
CREATE anchor:0 SET k = 1;
CREATE reading:1 SET v = 'old';
DEFINE FUNCTION fn::inner() { RETURN (SELECT VALUE v FROM ONLY reading:1); };
DEFINE FUNCTION fn::outer() { RETURN fn::inner(); };
SLEEP 50ms;
LET $ts = time::now();
SLEEP 50ms;
UPDATE reading:1 SET v = 'new';
DEFINE FUNCTION OVERWRITE fn::inner() { RETURN 'redefined'; };

-- The definitions at the stamp, the body's read at the stamp.
SELECT VALUE fn::outer() FROM anchor:0 VERSION $ts;

-- A subquery of the projection is under the statement's stamp.
SELECT VALUE (SELECT VALUE fn::outer() FROM anchor:0) FROM anchor:0 VERSION $ts;

-- The current definitions.
SELECT VALUE fn::outer() FROM anchor:0;
SELECT VALUE fn::outer() FROM anchor:0 VERSION $ts;
