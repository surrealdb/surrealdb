/**
[test]
reason = """
	Setup for relate_onto_a_hidden_edge_is_refused_whatever_it_names. Three edge
	tables a record user may create and update: `wrote`, whose `select`
	permission denies every record; `read`, whose `in` and `out` fields deny
	`select`; and `likes`, whose `select` permission admits only the edges the
	caller is the `in` of. Each holds a stored edge, and `likes` one the caller
	may read and one it may not. A record access signs the user in.
"""
run = false
*/

DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE book SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;

DEFINE TABLE wrote TYPE RELATION PERMISSIONS
	FOR select NONE,
	FOR create, update FULL;

DEFINE TABLE read TYPE RELATION PERMISSIONS FULL;
DEFINE FIELD OVERWRITE in ON read TYPE record PERMISSIONS FOR select NONE;
DEFINE FIELD OVERWRITE out ON read TYPE record PERMISSIONS FOR select NONE;

DEFINE TABLE likes TYPE RELATION PERMISSIONS
	FOR select WHERE in = $auth.id,
	FOR create, update FULL;

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );

CREATE user:alice, user:bob;
CREATE person:carol, person:mallory, book:diary, book:other;
RELATE person:carol->wrote:edge77->book:diary;
RELATE person:carol->read:edge77->book:diary;
RELATE user:alice->likes:own->book:diary;
RELATE user:bob->likes:theirs->book:diary;
