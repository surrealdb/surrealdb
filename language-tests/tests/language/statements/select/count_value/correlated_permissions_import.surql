/**
[test]
reason = """
	Setup for correlated_permissions: a table every record user may read, a table whose select
	permission reads the row, a table whose select permission is none, an outer table, and the
	record user the test runs as. Runs as owner.
"""
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
CREATE user:alice SET name = 'alice' RETURN NONE;
DEFINE TABLE outer SCHEMALESS PERMISSIONS FOR select FULL;
CREATE outer:1 SET num = 1 RETURN NONE;
CREATE outer:2 SET num = 2 RETURN NONE;
CREATE outer:3 SET num = 5 RETURN NONE;
DEFINE TABLE open SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD num ON open TYPE int;
DEFINE INDEX num ON open FIELDS num;
CREATE open:1 SET num = 1 RETURN NONE;
CREATE open:2 SET num = 1 RETURN NONE;
CREATE open:3 SET num = 2 RETURN NONE;
DEFINE TABLE owned SCHEMAFULL PERMISSIONS FOR select WHERE owner = $auth.id;
DEFINE FIELD owner ON owned TYPE record;
DEFINE FIELD num ON owned TYPE int;
DEFINE INDEX num ON owned FIELDS num;
CREATE owned:1 SET owner = user:alice, num = 1 RETURN NONE;
CREATE owned:2 SET owner = user:bob, num = 1 RETURN NONE;
CREATE owned:3 SET owner = user:alice, num = 2 RETURN NONE;
CREATE owned:4 SET owner = user:bob, num = 2 RETURN NONE;
CREATE owned:5 SET owner = user:bob, num = 5 RETURN NONE;
DEFINE TABLE denied SCHEMAFULL PERMISSIONS FOR select NONE;
DEFINE FIELD num ON denied TYPE int;
DEFINE INDEX num ON denied FIELDS num;
CREATE denied:1 SET num = 1 RETURN NONE;
CREATE denied:2 SET num = 2 RETURN NONE;
