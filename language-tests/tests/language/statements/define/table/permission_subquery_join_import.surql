/**
[test]
reason = """
	Setup for permission_subquery_join: documents whose select permission
	asks a grant table, readable only inside the permission, whether the
	caller holds a grant on the document, and the record user it is asked
	for.
"""
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:reader, user:other;
DEFINE TABLE doc SCHEMAFULL
	PERMISSIONS FOR select WHERE $auth.id IN (SELECT VALUE who FROM grant WHERE doc = $parent.id);
DEFINE FIELD title ON doc TYPE string;
DEFINE TABLE grant SCHEMALESS;
DEFINE TABLE note SCHEMAFULL
	PERMISSIONS FOR select WHERE $auth.id IN (SELECT VALUE who FROM grant WHERE kind = 'note');
DEFINE FIELD title ON note TYPE string;
CREATE doc:1 SET title = 'one';
CREATE doc:2 SET title = 'two';
CREATE doc:3 SET title = 'three';
CREATE grant:1 SET who = user:reader, doc = doc:1;
CREATE grant:2 SET who = user:other, doc = doc:2;
CREATE grant:3 SET who = user:reader, doc = doc:3;
CREATE grant:4 SET who = user:reader, kind = 'note';
CREATE note:1 SET title = 'n1';
