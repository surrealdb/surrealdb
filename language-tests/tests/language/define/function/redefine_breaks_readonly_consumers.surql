/**
[env]
namespace = true
database = true

[test]
reason = """
	COMPUTED field bodies (and permission guards) evaluate the functions they
	call on reads, under a frame that refuses writes, so those functions must
	stay read-only. The requirement is checked on whichever side is defined
	second: redefining a consumed function to write is refused (DEFINE
	OVERWRITE and ALTER alike, and through an intermediate function), naming
	the consumers; and a function name a COMPUTED body already references —
	late binding leaves it undefined until then — cannot be introduced as a
	writing function. Once the consumers are gone the same definitions are
	accepted.
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
error = "Cannot define function `fn::helper`: it modifies data, but it must remain read-only because it is used by COMPUTED field node.c"

[[test.results]]
error = "Cannot define function `fn::helper`: it modifies data, but it must remain read-only because it is used by COMPUTED field node.c"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define function `fn::ghost`: it modifies data, but it must remain read-only because it is used by COMPUTED field node.d"

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
*/

DEFINE TABLE log SCHEMALESS;
DEFINE TABLE node SCHEMALESS;
DEFINE FUNCTION fn::helper() { RETURN 1; };
DEFINE FUNCTION fn::relay() { RETURN fn::helper(); };

-- The field consumes fn::helper through fn::relay.
DEFINE FIELD c ON node COMPUTED fn::relay();

-- Redefining the leaf to write is refused, naming the consumer that reaches
-- it — through DEFINE OVERWRITE and ALTER alike.
DEFINE FUNCTION OVERWRITE fn::helper() { CREATE log; RETURN 1; };
ALTER FUNCTION fn::helper() { CREATE log; RETURN 1; };

-- Late binding: a COMPUTED body may reference a function that does not exist
-- yet, but the name can then only be introduced as a read-only function.
DEFINE FIELD d ON node COMPUTED fn::ghost();
DEFINE FUNCTION fn::ghost() { CREATE log; RETURN 1; };
DEFINE FUNCTION fn::ghost() { RETURN 2; };

-- An IF NOT EXISTS over an existing function is a no-op and stays one, even
-- with a writing body.
DEFINE FUNCTION IF NOT EXISTS fn::helper() { CREATE log; RETURN 1; };

-- With the consumers gone, the same redefinition is accepted.
REMOVE FIELD c ON node;
REMOVE FIELD d ON node;
DEFINE FUNCTION OVERWRITE fn::helper() { CREATE log; RETURN 1; };
