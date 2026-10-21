/**
[test]
reason = """
	Setup for record_id_list_reads_listed_records_permissions: a table whose
	`PERMISSIONS FOR select` admits the caller's own records and whose `secret`
	field only its owner reads, and two record users, `user:alice` and
	`user:bob`. Alice owns doc:1 and doc:3, Bob doc:2 and doc:4.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:alice;
CREATE user:bob;

DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth.id;
DEFINE FIELD secret ON doc PERMISSIONS FOR select WHERE owner = $auth.id;
CREATE doc:1 SET owner = user:alice, n = 1, secret = 'a1';
CREATE doc:2 SET owner = user:bob, n = 2, secret = 'b2';
CREATE doc:3 SET owner = user:alice, n = 3, secret = 'a3';
CREATE doc:4 SET owner = user:bob, n = 4, secret = 'b4';
