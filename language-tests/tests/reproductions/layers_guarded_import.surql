/**
[test]
reason = """
	Setup for layers_scopes, layers_param_shadow and
	layers_explain_redaction: a table whose `secret` field is readable
	only by the record's owner, judged through a computed field, a computed
	field whose own guard denies everyone, a user-defined function that reads
	`secret` off the row, an edge table with a guard of its own, a table
	whose own select permission admits only the record's owner, and the
	record user the tests run as. Runs as owner.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:owner;
CREATE user:reader;

DEFINE TABLE t SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD owner ON t TYPE record<user>;
DEFINE FIELD n ON t TYPE int;
-- Visible to everyone; the guard on `secret` reads it.
DEFINE FIELD is_owner ON t TYPE bool COMPUTED owner = $auth.id;
DEFINE FIELD secret ON t TYPE string PERMISSIONS FOR select WHERE is_owner;
-- Derived from a stored field the caller may read, denied to everyone.
DEFINE FIELD c ON t TYPE int COMPUTED n * 2 PERMISSIONS FOR select WHERE false;
-- Reads the row's `secret` in the frame of whoever calls it.
DEFINE FUNCTION fn::peek() { RETURN secret; } PERMISSIONS FULL;
CREATE t:1 SET owner = user:owner, secret = 'hidden', n = 21;

-- An edge table with its own guard, readable only from the edge's origin.
DEFINE TABLE owns TYPE RELATION IN user OUT t SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD secret ON owns TYPE string PERMISSIONS FOR select WHERE in = $auth.id;
RELATE user:owner->owns->t:1 SET secret = 'edge-hidden';

-- A table permission predicate of its own, readable only by the record's owner.
DEFINE TABLE gated SCHEMAFULL PERMISSIONS FOR select WHERE owner = $auth.id;
DEFINE FIELD owner ON gated TYPE record<user>;
CREATE gated:1 SET owner = user:owner;
