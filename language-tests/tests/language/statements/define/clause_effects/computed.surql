/**
[env]
namespace = true
database = true

[test]
reason = """
	A COMPUTED clause must not have effects: a write, written in it or reached
	through a stored function, a schema statement, a builtin that advances
	state outside the transaction, SLEEP, or a call whose effects cannot be
	known where the clause is defined. Each is refused where the field is
	defined, naming the clause and the construct. Randomness, the clock, a
	stored function that only reads and a closure written in the call are
	allowed. Nothing a refused definition holds runs.
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
error = "The COMPUTED clause must not have effects: `CREATE` writes data"

[[test.results]]
error = "The COMPUTED clause must not have effects: it calls `fn::writes`, in whose body `CREATE` writes data"

[[test.results]]
error = "The COMPUTED clause must not have effects: it calls `fn::relay`, which reaches `fn::writes`, in whose body `CREATE` writes data"

[[test.results]]
error = "The COMPUTED clause must not have effects: `DEFINE` changes the schema"

[[test.results]]
error = "The COMPUTED clause must not have effects: `sequence::nextval` advances state outside the transaction"

[[test.results]]
error = "The COMPUTED clause must not have effects: `SLEEP` blocks"

[[test.results]]
error = "The COMPUTED clause must not have effects: calling a closure value runs a body whose effects cannot be known where the clause is defined"

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
value = "[{ id: t:1, ok_fn: 1, ok_map: [2, 4], ok_now: true, ok_rand: 1 }]"

[[test.results]]
value = "['ok_fn', 'ok_map', 'ok_now', 'ok_rand']"

[[test.results]]
value = "[{ count: 0 }]"
*/

DEFINE SEQUENCE s;
DEFINE TABLE log SCHEMALESS;
DEFINE FUNCTION fn::writes() { CREATE log; RETURN 1; };
DEFINE FUNCTION fn::relay() { RETURN fn::writes(); };
DEFINE FUNCTION fn::reads($x: any) { RETURN $x; };
DEFINE TABLE t SCHEMALESS;

DEFINE FIELD c_create ON t COMPUTED (CREATE log RETURN NONE);
DEFINE FIELD c_fn ON t COMPUTED fn::writes();
DEFINE FIELD c_relay ON t COMPUTED fn::relay();
DEFINE FIELD c_ddl ON t COMPUTED { DEFINE TABLE other; 1 };
DEFINE FIELD c_nextval ON t COMPUTED sequence::nextval('s');
DEFINE FIELD c_sleep ON t COMPUTED { SLEEP 1ms; 1 };
DEFINE FIELD c_closure ON t COMPUTED $f(1);

DEFINE FIELD ok_rand ON t COMPUTED rand::int(1, 1);
DEFINE FIELD ok_now ON t COMPUTED type::is_datetime(time::now());
DEFINE FIELD ok_fn ON t COMPUTED fn::reads(1);
DEFINE FIELD ok_map ON t COMPUTED array::map([1, 2], |$v| $v * 2);

CREATE t:1 RETURN NONE;
SELECT * FROM t;
RETURN array::sort(object::keys((INFO FOR TABLE t).fields));
SELECT count() FROM log GROUP ALL;
