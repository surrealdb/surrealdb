/**
[env]
backend = ["mem", "rocksdb", "surrealkv"]
versioned = true
timeout = 5000

[test]
reason = """
A function called where a statement reads at a VERSION stamp runs its body at the
stamp, whether the body fetches the links of its rows or reads them through a
field: the links are read as of the stamp and not as the current records hold
them. Unversioned, the same calls read the current records.
"""

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
value = "[[{ id: holder:1, refs: [{ id: person:1, n: 1 }, { id: person:2, n: 2 }, { id: person:3, n: 3 }] }]]"

[[test.results]]
value = "[[[1, 2, 3]]]"

[[test.results]]
value = "[[{ id: holder:1, refs: [{ id: person:1, n: 101 }, { id: person:2, n: 102 }, { id: person:3, n: 103 }] }]]"

[[test.results]]
value = "[[[101, 102, 103]]]"

*/

DEFINE FUNCTION fn::f() { RETURN SELECT * FROM holder FETCH refs; };
DEFINE FUNCTION fn::g() { RETURN SELECT VALUE refs.n FROM holder; };
CREATE person:1 SET n = 1;
CREATE person:2 SET n = 2;
CREATE person:3 SET n = 3;
CREATE holder:1 SET refs = [person:1, person:2, person:3];
SLEEP 50ms;
LET $ts = time::now();
SLEEP 50ms;
UPDATE person SET n += 100;
SELECT VALUE fn::f() FROM holder VERSION $ts;
SELECT VALUE fn::g() FROM holder VERSION $ts;
SELECT VALUE fn::f() FROM holder;
SELECT VALUE fn::g() FROM holder;
