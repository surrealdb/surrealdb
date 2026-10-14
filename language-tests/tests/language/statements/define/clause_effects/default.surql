/**
[env]
namespace = true
database = true

[test]
reason = """
	A DEFAULT clause must not have effects: a write, written in it or reached
	through a stored function, a schema statement, SLEEP, or a call whose
	effects cannot be known where the clause is defined. Each is refused where
	the definition is made, naming the clause and the construct. Randomness,
	the clock and the advance of a sequence are allowed, as are a stored
	function that only reads and a closure written in the call. Nothing a
	refused definition holds runs.
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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The DEFAULT clause must not have effects: `CREATE` writes data"

[[test.results]]
error = "The DEFAULT clause must not have effects: it calls `fn::writes`, in whose body `CREATE` writes data"

[[test.results]]
error = "The DEFAULT clause must not have effects: it calls `fn::relay`, which reaches `fn::writes`, in whose body `CREATE` writes data"

[[test.results]]
error = "The DEFAULT clause must not have effects: `DEFINE` changes the schema"

[[test.results]]
error = "The DEFAULT clause must not have effects: `SLEEP` blocks"

[[test.results]]
error = "The DEFAULT clause must not have effects: calling a closure value runs a body whose effects cannot be known where the clause is defined"

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
value = "[]"

[[test.results]]
value = "[{ now: true, ok_fn: 1, ok_map: [2, 4], ok_next: 0, uuid: true }]"

[[test.results]]
value = "['ok_fn', 'ok_map', 'ok_next', 'ok_now', 'ok_uuid']"

[[test.results]]
value = "[{ count: 0 }]"
*/

DEFINE SEQUENCE s;
DEFINE TABLE log SCHEMALESS;
DEFINE FUNCTION fn::writes() { CREATE log; RETURN true; };
DEFINE FUNCTION fn::relay() { RETURN fn::writes(); };
DEFINE FUNCTION fn::reads($x: any) { RETURN $x; };
DEFINE TABLE t SCHEMALESS;

DEFINE FIELD f_create ON t DEFAULT (CREATE log RETURN NONE);
DEFINE FIELD f_fn ON t DEFAULT fn::writes();
DEFINE FIELD f_relay ON t DEFAULT fn::relay();
DEFINE FIELD f_ddl ON t DEFAULT { DEFINE TABLE other; true };
DEFINE FIELD f_sleep ON t DEFAULT { SLEEP 1ms; true };
DEFINE FIELD f_closure ON t DEFAULT $f(1);

DEFINE FIELD ok_uuid ON t DEFAULT rand::uuid();
DEFINE FIELD ok_next ON t DEFAULT sequence::nextval('s');
DEFINE FIELD ok_now ON t DEFAULT time::now();
DEFINE FIELD ok_fn ON t DEFAULT fn::reads(1);
DEFINE FIELD ok_map ON t DEFAULT array::map([1, 2], |$v| $v * 2);

CREATE t:1 RETURN NONE;
SELECT type::is_uuid(ok_uuid) AS uuid, type::is_datetime(ok_now) AS now, ok_next, ok_fn, ok_map FROM t;
RETURN array::sort(object::keys((INFO FOR TABLE t).fields));
SELECT count() FROM log GROUP ALL;
