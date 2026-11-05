/**
[test]
reason = """
	Setup for closure_call_guard, closure_callback_guard and
	closure_callback_guard_plan: a table whose `secret` field is denied to
	the record user outright while `public` is readable, and a record access
	to sign in with. `tags` is a readable array, the argument a closure-running
	builtin searches without running anything from it. Runs as owner.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:reader;

DEFINE TABLE t SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD public ON t TYPE string;
DEFINE FIELD secret ON t TYPE string PERMISSIONS FOR select NONE;
DEFINE FIELD tags ON t TYPE array<string>;

CREATE t:1 SET public = 'a', secret = 'yes', tags = ['x'];
CREATE t:2 SET public = 'b', secret = 'no', tags = ['y'];
