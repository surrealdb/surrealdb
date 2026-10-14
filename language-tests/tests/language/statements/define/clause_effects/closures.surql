/**
[env]
namespace = true
database = true

[test]
reason = """
	A closure a schema clause applies is judged by whether its body is in the
	clause. A closure written in the call, applied where it is written, or
	bound with LET in the clause and then called, has its body read like any
	other part of the clause: allowed when the body only reads, refused when
	it writes. A closure from a parameter the clause does not bind has a body
	that cannot be known where the clause is defined, and is refused, whether
	it is called, handed to a builtin, or handed to a method. A builtin method
	on the row that runs no closure is allowed.
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
error = "The COMPUTED clause must not have effects: `UPSERT` writes data"

[[test.results]]
error = "The COMPUTED clause must not have effects: calling a closure value runs a body whose effects cannot be known where the clause is defined"

[[test.results]]
error = "The COMPUTED clause must not have effects: `array::map` runs a closure whose effects cannot be known where the clause is defined"

[[test.results]]
error = "The VALUE clause must not have effects: the method `map` runs a closure whose effects cannot be known where the clause is defined"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ ok_applied: 4, ok_let: 2, ok_method: [3, 2] }]"

[[test.results]]
value = "['ok_applied', 'ok_let', 'ok_method', 'ok_plain_method']"

[[test.results]]
value = "[{ count: 0 }]"
*/

DEFINE TABLE log SCHEMALESS;
DEFINE TABLE t SCHEMALESS;

DEFINE FIELD ok_applied ON t COMPUTED (|$x| $x * 2)(2);
DEFINE FIELD ok_let ON t COMPUTED { LET $g = |$x| $x + 1; $g(1) };
DEFINE FIELD ok_method ON t VALUE [3, 1, 2].filter(|$v| $v > 1);
DEFINE FIELD ok_plain_method ON t ASSERT $value.is_none() OR $value.is_array();

DEFINE FIELD bad_let_writes ON t COMPUTED { LET $g = |$x| (CREATE log RETURN NONE); $g(1) };
DEFINE FIELD bad_written_writes ON t COMPUTED array::map([1], |$v| (UPSERT log:1 RETURN NONE));
DEFINE FIELD bad_param_call ON t COMPUTED $f(1);
DEFINE FIELD bad_param_builtin ON t COMPUTED array::map([1], $f);
DEFINE FIELD bad_param_method ON t VALUE [1].map($f);

CREATE t:1 RETURN NONE;
SELECT * OMIT id FROM t;
RETURN array::sort(object::keys((INFO FOR TABLE t).fields));
SELECT count() FROM log GROUP ALL;
