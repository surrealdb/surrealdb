/**
[test]
reason = """
	Setup for a_guard_cuts_what_the_row_holds: two fields guarded the
	same way, written with the same array of objects. `kept` keeps the
	declaration it was written under; `moved` is retyped to `object`
	afterwards, its element fields removed and its guard defined on the
	object, which rewrites no stored row. Runs as owner.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:probe;

DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;

DEFINE FIELD kept ON t TYPE array<object>;
DEFINE FIELD kept[*].secret ON t PERMISSIONS FOR select NONE;

DEFINE FIELD moved ON t TYPE array<object>;
DEFINE FIELD moved[*].secret ON t PERMISSIONS FOR select NONE;

CREATE t:1 SET
	kept = [{ secret: 'kept-a', ok: 1 }, { secret: 'kept-b', ok: 2 }],
	moved = [{ secret: 'moved-a', ok: 1 }, { secret: 'moved-b', ok: 2 }];

-- Only the object's own guard is left to cut the array the row still holds.
REMOVE FIELD moved[*] ON t;
DEFINE FIELD OVERWRITE moved ON t TYPE object;
DEFINE FIELD OVERWRITE moved.secret ON t PERMISSIONS FOR select NONE;
