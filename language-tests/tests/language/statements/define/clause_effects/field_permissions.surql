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
	is made, naming the clause and the construct. This covers a field's `select`, `create` and
	`update` clauses. A clause that reads, the clock and randomness are allowed.
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
error = "The PERMISSIONS clause must not have effects: it calls `fn::writes`, in whose body `CREATE` writes data"

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
DEFINE TABLE t SCHEMALESS;

DEFINE FIELD f_create ON t PERMISSIONS FOR select, create, update WHERE (CREATE log RETURN NONE);
DEFINE FIELD f_fn ON t PERMISSIONS FOR select, create, update WHERE fn::writes();
DEFINE FIELD f_relay ON t PERMISSIONS FOR select, create, update WHERE fn::relay();
DEFINE FIELD f_ddl ON t PERMISSIONS FOR select, create, update WHERE { DEFINE TABLE other; true };
DEFINE FIELD f_nextval ON t PERMISSIONS FOR select, create, update WHERE sequence::nextval('s');
DEFINE FIELD f_sleep ON t PERMISSIONS FOR select, create, update WHERE { SLEEP 1ms; true };
DEFINE FIELD f_closure ON t PERMISSIONS FOR select, create, update WHERE $f(1);
DEFINE FIELD f_update_only ON t PERMISSIONS FOR select FULL FOR update WHERE fn::writes();

DEFINE FIELD ok ON t PERMISSIONS FOR select WHERE fn::reads(true) AND time::now() > d'2000-01-01' FOR create, update WHERE rand::int(1, 1) = 1;

RETURN array::sort(object::keys((INFO FOR TABLE t).fields));
SELECT count() FROM log GROUP ALL;
