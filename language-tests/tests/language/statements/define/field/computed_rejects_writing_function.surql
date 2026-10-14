/**
[env]
namespace = true
database = true

[test]
reason = """
	A COMPUTED body is evaluated on every read of the field, so it must not
	modify data. DEFINE FIELD refuses a body whose effects include a write,
	whether the write is written directly, reached through a chain of
	user-defined functions, or is a schema change inside a callee. A body whose
	effects are read-only is accepted, and so is a sequence draw. A body that
	may write is refused as well: a call to a function that is not defined yet
	cannot be shown read-only. A callee redefined after the field is defined is
	not re-checked then, so the runtime frame refuses the write when a read
	reaches it. Nothing is written by any attempt. Each case uses its own table
	and no table with an accepted field is written or read after that field is
	defined, so no CREATE output derives a field another case defined.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define field `f` as `COMPUTED` bodies must be read-only."

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define field `g` as `COMPUTED` bodies must be read-only."

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define field `h` as `COMPUTED` bodies must be read-only."

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
error = "Cannot define field `late` as `COMPUTED` bodies must be read-only."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t7:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "A COMPUTED clause cannot contain a statement that modifies data"

[[test.results]]
value = "[{ count: 0 }]"
*/

DEFINE TABLE log SCHEMALESS;

-- A writing function called directly from the body.
DEFINE FUNCTION fn::w() { CREATE log SET n = 1; RETURN 1; };
DEFINE FIELD f ON t1 COMPUTED fn::w();

-- The write is reached through another function.
DEFINE FUNCTION fn::outer() { RETURN fn::w(); };
DEFINE FIELD g ON t2 COMPUTED fn::outer();

-- A schema change inside the callee counts as a write.
DEFINE FUNCTION fn::d() { DEFINE TABLE smuggled; RETURN 1; };
DEFINE FIELD h ON t3 COMPUTED fn::d();

-- A read-only function is accepted.
DEFINE FUNCTION fn::r($x: int) { RETURN $x * 2; };
DEFINE FIELD ok ON t4 COMPUTED fn::r(2);

-- A sequence draw is accepted.
DEFINE SEQUENCE s;
DEFINE FUNCTION fn::n() { RETURN sequence::nextval('s'); };
DEFINE FIELD sq ON t5 COMPUTED fn::n();

-- A function that is not defined yet may write, so it is refused.
DEFINE FIELD late ON t6 COMPUTED fn::not_yet();

-- A callee redefined after the field is defined is refused at read time. The
-- row is created before the field so its output derives nothing.
DEFINE FUNCTION fn::r2() { RETURN 1; };
CREATE t7:1;
DEFINE FIELD re ON t7 COMPUTED fn::r2();
DEFINE FUNCTION OVERWRITE fn::r2() { CREATE log SET n = 2; RETURN 1; };
SELECT re FROM t7;

-- Nothing was written by any attempt.
SELECT count() FROM log GROUP ALL;
