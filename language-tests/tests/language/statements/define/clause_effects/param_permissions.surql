/**
[env]
namespace = true
database = true

[test]
reason = """
	A PERMISSIONS clause must not have effects: a write, written in it or reached
	through a stored function, a schema statement, a builtin that advances
	state outside the transaction, SLEEP, or a call whose effects cannot be
	known where the clause is defined. Each is refused where the definition
	is made, naming the clause and the construct. This covers a parameter's `PERMISSIONS`
	clause. A clause that reads, the clock and randomness are allowed.
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
error = "The PERMISSIONS clause must not have effects: `CREATE` writes data"

[[test.results]]
error = "The PERMISSIONS clause must not have effects: it calls `fn::writes`, in whose body `CREATE` writes data"

[[test.results]]
error = "The PERMISSIONS clause must not have effects: it calls `fn::relay`, which reaches `fn::writes`, in whose body `CREATE` writes data"

[[test.results]]
error = "The PERMISSIONS clause must not have effects: `DEFINE` changes the schema"

[[test.results]]
error = "The PERMISSIONS clause must not have effects: `sequence::nextval` advances state outside the transaction"

[[test.results]]
error = "The PERMISSIONS clause must not have effects: `SLEEP` blocks"

[[test.results]]
error = "The PERMISSIONS clause must not have effects: calling a closure value runs a body whose effects cannot be known where the clause is defined"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['ok']"

[[test.results]]
value = "[{ count: 0 }]"
*/

DEFINE SEQUENCE s;
DEFINE TABLE log SCHEMALESS;
DEFINE FUNCTION fn::writes() { CREATE log; RETURN true; };
DEFINE FUNCTION fn::relay() { RETURN fn::writes(); };
DEFINE FUNCTION fn::reads($x: any) { RETURN $x; };

DEFINE PARAM $p_create VALUE 1 PERMISSIONS WHERE (CREATE log RETURN NONE);
DEFINE PARAM $p_fn VALUE 1 PERMISSIONS WHERE fn::writes();
DEFINE PARAM $p_relay VALUE 1 PERMISSIONS WHERE fn::relay();
DEFINE PARAM $p_ddl VALUE 1 PERMISSIONS WHERE { DEFINE TABLE other; true };
DEFINE PARAM $p_nextval VALUE 1 PERMISSIONS WHERE sequence::nextval('s');
DEFINE PARAM $p_sleep VALUE 1 PERMISSIONS WHERE { SLEEP 1ms; true };
DEFINE PARAM $p_closure VALUE 1 PERMISSIONS WHERE $f(1);

DEFINE PARAM $ok VALUE 1 PERMISSIONS WHERE fn::reads(true) AND time::now() > d'2000-01-01' AND rand::int(1, 1) = 1;

RETURN array::sort(object::keys((INFO FOR DB).params));
SELECT count() FROM log GROUP ALL;
