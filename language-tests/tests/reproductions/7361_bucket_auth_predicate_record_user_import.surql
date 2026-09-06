/**
[test]
reason = "Setup for issue 7361: a record user and bucket permission predicate that dereferences $auth."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid );

CREATE user:bob SET is_admin = false;

DEFINE BUCKET auth_gate BACKEND "memory"
	PERMISSIONS WHERE $auth = NONE OR $auth.is_admin = true;

f"auth_gate:/secret.txt".put("SECRET");

DEFINE FUNCTION fn::read_auth_gate($file: file) -> string {
	RETURN <string>file::get($file);
} PERMISSIONS FULL;
