/**
[env]
namespace = true
database = true

[env.capabilities]
allow-eval-query = ["system"]

[test]
reason = """
	A COMPUTED body is evaluated on every read of the field, under the definer's
	auth rather than the reader's, so it must not modify data. The
	definition-time check resolves user-defined function calls against their
	stored bodies, so a write behind a call is refused at definition, naming the
	function. What definition time cannot see — here an `eval` body, whose query
	string is only a value — is refused by the runtime frame instead.

	The row is created before the field is defined, so the SELECTs below fail on
	the refused write rather than on there being nothing to read. Bare and
	wrapped reads must agree: before the runtime frame existed, the bare one
	failed on its read-only transaction while the wrapped one succeeded and left
	a row in `log`.
"""
issue = 856

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define field `direct` as `COMPUTED` bodies must be read-only."

[[test.results]]
error = "Cannot define field `indirect` as `COMPUTED`: the body calls `fn::bump`, which modifies data, and `COMPUTED` bodies must be read-only"

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

-- Written directly into the body, the mutation is caught at definition time.
DEFINE FIELD direct ON node COMPUTED (CREATE log SET n = 1);

-- Behind a function call it is caught at definition time too, by resolving
-- the callee's stored body, and the error names the function.
DEFINE FIELD indirect ON node COMPUTED fn::bump();

-- An `eval` body is opaque to definition time: the query string is a value.
DEFINE FUNCTION fn::opaque() { RETURN eval::surql("CREATE log SET n = 1"); };

CREATE node:1;

-- So the field defines successfully...
DEFINE FIELD hidden ON node COMPUTED fn::opaque();

-- ...and reading it is refused by the runtime frame, bare and inside a
-- transaction alike.
SELECT hidden FROM node;

BEGIN;
SELECT hidden FROM node;
COMMIT;

-- Nothing was written by any attempt.
SELECT count() FROM log GROUP ALL;
