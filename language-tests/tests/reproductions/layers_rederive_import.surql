/**
[test]
reason = """
	Setup for layers_rederive_frame: a table whose computed fields
	reach a restricted field of their own record through its id and a
	restricted field of a linked record, each under its own guard, and the
	record user the test runs as. Runs as owner.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:reader;

DEFINE TABLE friend SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD name ON friend TYPE string;
DEFINE FIELD hidden ON friend TYPE string PERMISSIONS FOR select NONE;
CREATE friend:1 SET name = 'f', hidden = 'friend-secret';

DEFINE TABLE item SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD g ON item TYPE string PERMISSIONS FOR select NONE;
DEFINE FIELD f ON item TYPE record<friend>;
-- Reads the record's own restricted field through its id, under a guard that
-- admits the caller.
DEFINE FIELD self_g ON item TYPE option<string> COMPUTED id.g
	PERMISSIONS FOR select WHERE $auth.id = user:reader;
-- Reads a restricted field of the linked record, under the same guard.
DEFINE FIELD link_hidden ON item TYPE option<string> COMPUTED f.hidden
	PERMISSIONS FOR select WHERE $auth.id = user:reader;
-- The same reads, unguarded.
DEFINE FIELD self_g_open ON item TYPE option<string> COMPUTED id.g;
DEFINE FIELD link_hidden_open ON item TYPE option<string> COMPUTED f.hidden;
-- A body reading the restricted field by name, evaluated against the record
-- as the caller may read it: `g` is absent there, so the length is 0, and the
-- guard admits the field on that value alone.
DEFINE FIELD g_len ON item TYPE int COMPUTED string::len(g ?? '')
	PERMISSIONS FOR select WHERE $value = 0;
CREATE item:1 SET g = 'x', f = friend:1;
