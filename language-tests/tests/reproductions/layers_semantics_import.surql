/**
[test]
reason = """
	Setup for layers_runtime_semantics: a table whose computed fields
	read, dereference and are guarded by a stored field the record user may
	not select; a table with a guard on a wildcard path; and two tables with
	a guarded field each, read together. Runs as owner.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:reader;
CREATE user:owner;

DEFINE TABLE item SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD shown ON item TYPE int;
DEFINE FIELD g ON item TYPE string PERMISSIONS FOR select NONE;
-- Guarded on the denied stored field: judged against the stored value.
DEFINE FIELD c_raw ON item TYPE string COMPUTED 'C' PERMISSIONS FOR select WHERE g != 'x';
-- Reads the denied stored field: derived again once the field is cut.
DEFINE FIELD c_reads ON item TYPE bool COMPUTED g IS NONE;
-- Dereferences its own record: reads the row as the caller may see it.
DEFINE FIELD c_self ON item TYPE option<string> COMPUTED id.g;
-- Guarded on the caller: admitted for the record user.
DEFINE FIELD c_pass ON item TYPE string COMPUTED 'P' PERMISSIONS FOR select WHERE $auth.id = user:reader;
CREATE item:1 SET shown = 1, g = 'x';

DEFINE TABLE bag SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD arr ON bag TYPE array<object>;
DEFINE FIELD arr[*].shown ON bag TYPE int;
DEFINE FIELD arr[*].secret ON bag TYPE option<string> PERMISSIONS FOR select NONE;
CREATE bag:1 SET arr = [{ shown: 1, secret: 's1' }, { shown: 2, secret: 's2' }];

DEFINE TABLE a SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD pub ON a TYPE string;
DEFINE FIELD hid_a ON a TYPE string PERMISSIONS FOR select NONE;
DEFINE TABLE b SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD pub ON b TYPE string;
DEFINE FIELD hid_b ON b TYPE string PERMISSIONS FOR select WHERE $auth.id = user:owner;
CREATE a:1 SET pub = 'a', hid_a = 'HA';
CREATE b:1 SET pub = 'b', hid_b = 'HB';
