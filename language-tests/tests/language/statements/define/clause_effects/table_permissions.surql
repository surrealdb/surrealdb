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
	is made, naming the clause and the construct. This covers a table's `select`, `create`,
	`update` and `delete` clauses. A clause that reads, the clock and randomness are allowed.
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
error = "The PERMISSIONS clause must not have effects: it calls `fn::writes`, in whose body `CREATE` writes data"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['log', 'ok']"

[[test.results]]
value = "[{ count: 0 }]"
*/

DEFINE SEQUENCE s;
DEFINE TABLE log SCHEMALESS;
DEFINE FUNCTION fn::writes() { CREATE log; RETURN true; };
DEFINE FUNCTION fn::relay() { RETURN fn::writes(); };
DEFINE FUNCTION fn::reads($x: any) { RETURN $x; };

DEFINE TABLE t_create PERMISSIONS FOR select, create, update, delete WHERE (CREATE log RETURN NONE);
DEFINE TABLE t_fn PERMISSIONS FOR select, create, update, delete WHERE fn::writes();
DEFINE TABLE t_relay PERMISSIONS FOR select, create, update, delete WHERE fn::relay();
DEFINE TABLE t_ddl PERMISSIONS FOR select, create, update, delete WHERE { DEFINE TABLE other; true };
DEFINE TABLE t_nextval PERMISSIONS FOR select, create, update, delete WHERE sequence::nextval('s');
DEFINE TABLE t_sleep PERMISSIONS FOR select, create, update, delete WHERE { SLEEP 1ms; true };
DEFINE TABLE t_closure PERMISSIONS FOR select, create, update, delete WHERE $f(1);
DEFINE TABLE t_delete_only PERMISSIONS FOR select FULL FOR delete WHERE fn::writes();

DEFINE TABLE ok PERMISSIONS FOR select WHERE fn::reads(true) AND time::now() > d'2000-01-01' FOR create, update, delete WHERE rand::int(1, 1) = 1;

RETURN array::sort(object::keys((INFO FOR DB).tables));
SELECT count() FROM log GROUP ALL;
