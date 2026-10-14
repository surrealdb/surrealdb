/**
[env]
namespace = true
database = true

[test]
reason = """
	A COMPUTED body is evaluated on every read of the field, under the definer's
	auth rather than the reader's, so it must not modify data (register #17).
	DEFINE FIELD rejects a mutation written directly into the body, and a body
	whose user-defined callees may write: the callees are checked against the
	catalog as it stands when the field is defined, so a callee that writes is
	refused there.

	A callee redefined after the field is defined is not checked again, and may
	reach a write only on inputs this field never supplies. The runtime frame
	refuses the write when a read actually reaches it, which is what the reads
	of `indirect2` pin.

	The row is created before the field is defined, so the SELECTs below fail on
	the refused write rather than on there being nothing to read. Bare and
	wrapped reads must agree.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: node:1 }]"

[[test.results]]
error = "The COMPUTED clause must not have effects: it calls `fn::bump`, in whose body `CREATE` writes data"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The COMPUTED clause must not have effects: `CREATE` writes data"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The COMPUTED clause must not have effects: `CREATE` writes data"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "[{ count: 0 }]"
*/

DEFINE TABLE log SCHEMALESS;
DEFINE TABLE node SCHEMALESS;
DEFINE FUNCTION fn::bump() { CREATE log SET n = 1; RETURN 1; };

CREATE node:1;

-- The callee writes when the field is defined, so the definition is refused.
DEFINE FIELD indirect ON node COMPUTED fn::bump();

-- A callee that is read-only when the field is defined is accepted, and is
-- redefined with the write afterwards.
DEFINE FUNCTION fn::bump2() { RETURN 1; };
DEFINE FIELD indirect2 ON node COMPUTED fn::bump2();
DEFINE FUNCTION OVERWRITE fn::bump2() { CREATE log SET n = 1; RETURN 1; };

-- Reading it is refused by the runtime frame, bare and inside a transaction
-- alike.
SELECT indirect2 FROM node;

BEGIN;
SELECT indirect2 FROM node;
COMMIT;

-- Nothing was written by any attempt.
SELECT count() FROM log GROUP ALL;
