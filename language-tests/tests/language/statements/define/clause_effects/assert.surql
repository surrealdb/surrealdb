/**
[env]
namespace = true
database = true

[test]
reason = """
	A ASSERT clause must not have effects: a write, written in it or reached
	through a stored function, a schema statement, a builtin that advances
	state outside the transaction, SLEEP, or a call whose effects cannot be
	known where the clause is defined. Each is refused where the definition
	is made, naming the clause and the construct. Randomness, the clock, a stored function that only
	reads and a closure written in the call are allowed. Nothing a refused
	definition holds runs.
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
error = "The ASSERT clause must not have effects: `CREATE` writes data"

[[test.results]]
error = "The ASSERT clause must not have effects: it calls `fn::writes`, in whose body `CREATE` writes data"

[[test.results]]
error = "The ASSERT clause must not have effects: it calls `fn::relay`, which reaches `fn::writes`, in whose body `CREATE` writes data"

[[test.results]]
error = "The ASSERT clause must not have effects: `DEFINE` changes the schema"

[[test.results]]
error = "The ASSERT clause must not have effects: `sequence::nextval` advances state outside the transaction"

[[test.results]]
error = "The ASSERT clause must not have effects: `SLEEP` blocks"

[[test.results]]
error = "The ASSERT clause must not have effects: calling a closure value runs a body whose effects cannot be known where the clause is defined"

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
value = "[{ id: t:1, ok_fn: 1, ok_map: 1, ok_now: 1, ok_rand: 1 }]"

[[test.results]]
value = "['ok_fn', 'ok_map', 'ok_now', 'ok_rand']"

[[test.results]]
value = "[{ count: 0 }]"
*/

DEFINE SEQUENCE s;
DEFINE TABLE log SCHEMALESS;
DEFINE FUNCTION fn::writes() { CREATE log; RETURN true; };
DEFINE FUNCTION fn::relay() { RETURN fn::writes(); };
DEFINE FUNCTION fn::reads($x: any) { RETURN $x; };
DEFINE TABLE t SCHEMALESS;

DEFINE FIELD f_create ON t ASSERT (CREATE log RETURN NONE);
DEFINE FIELD f_fn ON t ASSERT fn::writes();
DEFINE FIELD f_relay ON t ASSERT fn::relay();
DEFINE FIELD f_ddl ON t ASSERT { DEFINE TABLE other; true };
DEFINE FIELD f_nextval ON t ASSERT sequence::nextval('s');
DEFINE FIELD f_sleep ON t ASSERT { SLEEP 1ms; true };
DEFINE FIELD f_closure ON t ASSERT $f(1);

DEFINE FIELD ok_rand ON t ASSERT rand::int(1, 1) = 1;
DEFINE FIELD ok_now ON t ASSERT time::now() > d'2000-01-01';
DEFINE FIELD ok_fn ON t ASSERT fn::reads(true);
DEFINE FIELD ok_map ON t ASSERT array::len(array::map([1, 2], |$v| $v * 2)) = 2;

CREATE t:1 SET ok_rand = 1, ok_now = 1, ok_fn = 1, ok_map = 1 RETURN NONE;
SELECT * FROM t;
RETURN array::sort(object::keys((INFO FOR TABLE t).fields));
SELECT count() FROM log GROUP ALL;
