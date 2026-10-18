/**
[test]
reason = "Setup for 7361_bucket_permission_reads_auth.surql: a bucket whose permission predicate dereferences `$auth`, over a user table the caller cannot read, and a seeded file."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid );

DEFINE TABLE user SCHEMALESS PERMISSIONS NONE;
CREATE user:bob SET is_admin = false;

DEFINE BUCKET b BACKEND "memory"
	PERMISSIONS WHERE $auth = NONE OR $auth.is_admin = true;
f"b:/x.md".put("7878");

DEFINE FUNCTION fn::read($f: file) -> string { RETURN <string>file::get($f) } PERMISSIONS FULL;
