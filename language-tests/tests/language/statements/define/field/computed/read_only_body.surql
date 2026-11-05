/**
[env]
namespace = true
database = true

[test]
reason = "A COMPUTED body is evaluated on every read of the field, inside the reading statement's own transaction, which is read-only for a plain SELECT. A write in the body can therefore never succeed -- it only turns every later read of the field into an error. Register #28 refuses a data-modifying statement written directly in the body at plan time, as region_constraints/computed_write.surql pins, so every body here is read-only. A write reached through a function call is refused at definition too, by checking the callees against the catalog as it stands then (register #17; see write_via_function); a callee redefined afterwards is caught by the runtime frame at read time. Read-only bodies, including calls to read-only user-defined functions, are unaffected."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: thing:1, doubled: 4 }]"
*/

DEFINE TABLE thing SCHEMALESS;

-- A read-only body, including a call to a read-only user-defined function,
-- is unaffected.
DEFINE FUNCTION fn::double($x: int) { RETURN $x * 2; };
DEFINE FIELD doubled ON thing COMPUTED fn::double(2);
CREATE thing:1;
