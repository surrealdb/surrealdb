/**
[env]
backend = ["rocksdb", "surrealkv"]
versioned = true
timeout = 5000

[test]
reason = """
Code compiled apart from the statement that reaches it runs in the frame of the statement that
calls it: a function that calls a function, a function that binds the rows of a FETCH to a
variable, a function that maps a closure over a subquery, a closure bound by LET and called from
a versioned statement, and a subquery nested in a versioned statement read their links as of the
VERSION of the statement, and as the current records hold them where it has none.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:1, n: 1 }]"

[[test.results]]
value = "[{ id: person:2, n: 2 }]"

[[test.results]]
value = "[{ id: person:3, n: 3 }]"

[[test.results]]
value = "[{ id: holder:1, refs: [person:1, person:2, person:3] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:1, n: 101 }, { id: person:2, n: 102 }, { id: person:3, n: 103 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[{ id: holder:1, refs: [{ id: person:1, n: 1 }, { id: person:2, n: 2 }, { id: person:3, n: 3 }] }]]"

[[test.results]]
value = "[[{ id: holder:1, refs: [{ id: person:1, n: 1 }, { id: person:2, n: 2 }, { id: person:3, n: 3 }] }]]"

[[test.results]]
value = "[[[[1, 2, 3]], [[1, 2, 3]]]]"

[[test.results]]
value = "[[{ id: holder:1, refs: [{ id: person:1, n: 1 }, { id: person:2, n: 2 }, { id: person:3, n: 3 }] }]]"

[[test.results]]
value = "[[{ id: holder:1, refs: [{ id: person:1, n: 1 }, { id: person:2, n: 2 }, { id: person:3, n: 3 }] }]]"

[[test.results]]
value = "[[{ id: holder:1, refs: [{ id: person:1, n: 101 }, { id: person:2, n: 102 }, { id: person:3, n: 103 }] }]]"

[[test.results]]
value = "[[{ id: holder:1, refs: [{ id: person:1, n: 101 }, { id: person:2, n: 102 }, { id: person:3, n: 103 }] }]]"

[[test.results]]
value = "[[[[101, 102, 103]], [[101, 102, 103]]]]"

[[test.results]]
value = "[[{ id: holder:1, refs: [{ id: person:1, n: 101 }, { id: person:2, n: 102 }, { id: person:3, n: 103 }] }]]"

*/
DEFINE FUNCTION fn::f() { RETURN SELECT * FROM holder FETCH refs; };
DEFINE FUNCTION fn::outer() { RETURN fn::f(); };
DEFINE FUNCTION fn::loop_() { LET $r = (SELECT * FROM holder FETCH refs); RETURN $r; };
DEFINE FUNCTION fn::map() { RETURN array::map([1, 2], |$x| (SELECT VALUE refs.n FROM holder)); };
CREATE person:1 SET n = 1;
CREATE person:2 SET n = 2;
CREATE person:3 SET n = 3;
CREATE holder:1 SET refs = [person:1, person:2, person:3];
SLEEP 50ms;
LET $ts = time::now();
SLEEP 50ms;
UPDATE person SET n += 100;
LET $c = || { SELECT * FROM holder FETCH refs };
SELECT VALUE fn::outer() FROM holder VERSION $ts;
SELECT VALUE fn::loop_() FROM holder VERSION $ts;
SELECT VALUE fn::map() FROM holder VERSION $ts;
SELECT VALUE $c() FROM holder VERSION $ts;
SELECT VALUE (SELECT * FROM holder FETCH refs) FROM holder VERSION $ts;
SELECT VALUE fn::outer() FROM holder;
SELECT VALUE fn::loop_() FROM holder;
SELECT VALUE fn::map() FROM holder;
SELECT VALUE $c() FROM holder;
