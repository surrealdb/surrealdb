/**
[env]
backend = ["mem", "rocksdb"]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A write after a schema statement whose effect depends on the data — under a
	condition, or in a loop — runs each record through the definitions the
	catalog holds when the record is written: a field defined on one iteration
	types the writes after it, a unique index refuses a duplicate written after
	it, an event and a view defined mid-loop observe the writes after them, and
	an update and a delete after a redefinition apply it.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:0, n: '0' }, { id: t:1, n: '1' }, { id: t:2, n: '2' }, { id: t:3, n: '3' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:0, m: 0 }, { id: t:1, m: 1 }, { id: t:2, m: 2 }, { id: t:3, m: 3 }]"

[[test.results]]
error = "Database index `u` already contains 1, with record `uq:1`"

[[test.results]]
error = "The table 'uq' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: log:ev, n: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 3, id: vw:[] }]"

[[test.results]]
value = "[{ id: up:1, n: 1 }]"

[[test.results]]
value = "[{ id: up:1, n: 'x' }]"

[[test.results]]
error = "Found 'z' for field `n`, with record `up:1`, but field must conform to: $value = 'y'"

[[test.results]]
value = "[{ id: owner:1 }]"

[[test.results]]
value = "[]"
*/
DEFINE TABLE t;
FOR $i IN 0..4 { IF $i = 2 { DEFINE FIELD n ON t TYPE string }; CREATE type::record('t', $i) SET n = <string> $i };
SELECT * FROM t ORDER BY id;
FOR $i IN 0..4 { IF $i = 2 { DEFINE FIELD m ON t TYPE int }; UPDATE type::record('t', $i) SET m = $i };
SELECT id, m FROM t ORDER BY id;
FOR $i IN 0..4 { IF $i = 1 { DEFINE INDEX u ON uq FIELDS k UNIQUE }; CREATE type::record('uq', $i) SET k = $i % 2 };
SELECT * FROM uq ORDER BY id;
FOR $i IN 0..4 { CREATE type::record('ev', $i) SET n = $i; IF $i = 1 { DEFINE EVENT e ON ev THEN (UPSERT log:ev SET n += 1) } };
SELECT * FROM log;
FOR $i IN 0..3 { IF $i = 1 { DEFINE TABLE vw AS SELECT count() AS c FROM src GROUP ALL }; CREATE type::record('src', $i) };
SELECT * FROM vw;
CREATE up:1 SET n = 1;
{ IF (SELECT VALUE n FROM ONLY up:1) = 1 { DEFINE FIELD n ON up TYPE string ASSERT $value != '' }; UPDATE up:1 SET n = 'x'; RETURN SELECT * FROM up };
{ IF (SELECT VALUE n FROM ONLY up:1) = 'x' { DEFINE FIELD OVERWRITE n ON up TYPE string ASSERT $value = 'y' }; UPDATE up:1 SET n = 'z' };
CREATE owner:1;
{ IF true { DEFINE FIELD who ON pet TYPE option<record<owner>> REFERENCE ON DELETE CASCADE }; CREATE pet:1 SET who = owner:1; DELETE owner:1; RETURN SELECT * FROM pet };
