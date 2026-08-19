/**
[env]
namespace = true
database = true

[test]
reason = """
	A COMPUTED body is evaluated on every read of the field, under the definer's
	auth rather than the reader's, so it must not modify data. DEFINE FIELD
	rejects a mutation written directly into the body, but a call to a
	user-defined function is opaque to that check: the callee's body lives in
	the catalog, can be redefined afterwards, and may reach a write only on
	inputs this field never supplies. So a body that calls a writing function
	defines successfully, and the runtime frame refuses the write when a read
	actually reaches it.

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
value = "NONE"

[[test.results]]
error = "A COMPUTED clause cannot contain a statement that modifies data"

[[test.results]]
value = "NONE"

[[test.results]]
error = "A COMPUTED clause cannot contain a statement that modifies data"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "[{ count: 0 }]"
*/

DEFINE TABLE log SCHEMALESS;
DEFINE TABLE node SCHEMALESS;
DEFINE FUNCTION fn::bump() { CREATE log SET n = 1; RETURN 1; };

CREATE node:1;

-- Behind a function call the write is invisible at definition time, so the
-- field defines successfully.
DEFINE FIELD indirect ON node COMPUTED fn::bump();

-- Reading it is refused by the runtime frame, bare and inside a transaction
-- alike.
SELECT indirect FROM node;

BEGIN;
SELECT indirect FROM node;
COMMIT;

-- Nothing was written by any attempt.
SELECT count() FROM log GROUP ALL;
