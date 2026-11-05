/**
[test]
reason = """
	Setup for relate_override_error_hides_the_stored_endpoint. An edge table a
	record user may create and update but not SELECT (FOR select NONE), seeded
	with a hidden edge person:carol->wrote:edge77->book:diary. A record access
	signs the user in.
"""
run = false
*/

DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE book SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE wrote TYPE RELATION PERMISSIONS
	FOR select NONE,
	FOR create, update FULL;

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );

CREATE user:alice;
CREATE person:carol, person:mallory, book:diary, book:other;
RELATE person:carol->wrote:edge77->book:diary;
