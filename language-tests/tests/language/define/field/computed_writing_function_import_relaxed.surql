/**
[env]
namespace = true
database = true

[test]
reason = """
	The definition-time checks that resolve function calls (COMPUTED bodies,
	permission guards, and the consumer check on function redefinition) are
	relaxed under import: an export taken before those checks existed can
	contain a computed field calling a writing function, and restoring it must
	not fail. The runtime frame still refuses the write on every read, exactly
	as it did on the database the export came from.
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
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "A COMPUTED clause cannot contain a statement that modifies data"

[[test.results]]
value = "[{ count: 0 }]"
*/

OPTION IMPORT;
DEFINE TABLE log SCHEMALESS;
DEFINE TABLE node SCHEMALESS;
DEFINE FUNCTION fn::bump() { CREATE log SET n = 1; RETURN 1; };

-- All three call-resolving checks stand down under import: the computed body
-- calling a writing function, the guard calling one, and the redefinition of
-- a function that consumers require to stay read-only.
DEFINE FIELD indirect ON node COMPUTED fn::bump();
DEFINE TABLE doc PERMISSIONS FOR select WHERE fn::bump();
DEFINE FUNCTION OVERWRITE fn::bump() { CREATE log SET n = 2; RETURN 2; };

CREATE node:1;

-- The runtime frame is not relaxed: reading the field still refuses the write.
SELECT indirect FROM node;

-- Nothing was written.
SELECT count() FROM log GROUP ALL;
