/**
[env]
namespace = true
database = true

[test]
reason = "A COMPUTED body is evaluated on every read of the field, inside the reading statement's own transaction, which is read-only for a plain SELECT. A write in the body can therefore never succeed -- it only turns every later read of the field into an error, and the two engines phrase that error differently. DEFINE FIELD rejects a direct write in the body instead. A write reached through a function call is still accepted here, because the callee can be redefined afterwards so definition time cannot be sound about it; that one keeps failing at read time. Read-only bodies, including calls to read-only user-defined functions, are unaffected."

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define field `c` as `COMPUTED` bodies must be read-only."

[[test.results]]
error = "Cannot define field `c` as `COMPUTED` bodies must be read-only."

[[test.results]]
error = "Cannot define field `c` as `COMPUTED` bodies must be read-only."

[[test.results]]
error = "Cannot define field `c` as `COMPUTED` bodies must be read-only."

[[test.results]]
error = "Cannot define field `c` as `COMPUTED` bodies must be read-only."

[[test.results]]
error = "Cannot define field `c` as `COMPUTED` bodies must be read-only."

[[test.results]]
error = "Cannot define field `c` as `COMPUTED` bodies must be read-only."

[[test.results]]
error = "Cannot define field `c` as `COMPUTED` bodies must be read-only."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: thing:1, doubled: 4 }]"
*/

DEFINE TABLE thing SCHEMALESS;

-- Every data-modifying statement is refused directly in the body.
DEFINE FIELD c ON thing COMPUTED (CREATE other:1);
DEFINE FIELD c ON thing COMPUTED (UPDATE other:1 SET n = 1);
DEFINE FIELD c ON thing COMPUTED (UPSERT other:1 SET n = 1);
DEFINE FIELD c ON thing COMPUTED (DELETE other:1);
DEFINE FIELD c ON thing COMPUTED (INSERT INTO other { n: 1 });
DEFINE FIELD c ON thing COMPUTED (RELATE thing:1->edge->thing:2);
DEFINE FIELD c ON thing COMPUTED (DEFINE FIELD sneaky ON other TYPE int);

-- A write reached through an idiom or a subquery is refused too: unlike a
-- PERMISSIONS clause, a COMPUTED body has no runtime guard behind it, so the
-- whole tree is walked.
DEFINE FIELD c ON thing COMPUTED (1 + (CREATE other:1).n);

-- A read-only body, including a call to a read-only user-defined function,
-- is unaffected.
DEFINE FUNCTION fn::double($x: int) { RETURN $x * 2; };
DEFINE FIELD doubled ON thing COMPUTED fn::double(2);
CREATE thing:1;
