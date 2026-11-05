/**
[test]
reason = """
	Setup for computed_under_where_permission, its _root and its _admin
	variants: a table with a COMPUTED field the record user may see and three
	it may not — denied outright, denied on a stored fact of the session, and
	denied to every caller — each of which raises if its body runs, and a table
	whose one COMPUTED field is guarded by a stored fact of the session and
	throws through a stored function when its body runs. Runs as owner.

	Every record is created before any COMPUTED field is defined: a CREATE's
	output derives every computed field as the importing principal, which would
	raise. `fn::r` is read-only when the fields calling it are defined and is
	redefined with a write afterwards, so the definition-time check accepts the
	fields and any read that derives one of them raises. Nothing after the
	redefinition writes `doc`.
"""
run = false
*/

DEFINE TABLE doc PERMISSIONS FULL;
DEFINE TABLE refs PERMISSIONS FULL;
DEFINE TABLE gate PERMISSIONS FULL;
DEFINE TABLE log;
CREATE doc:1;
CREATE refs:1 SET link = doc:1;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE gate:1;
CREATE user:reader SET admin = false;
CREATE user:boss SET admin = true;

DEFINE FUNCTION fn::r() { RETURN 1; };
DEFINE FIELD shown ON doc COMPUTED 'shown';
DEFINE FIELD hidden ON doc COMPUTED fn::r() PERMISSIONS FOR select NONE;
DEFINE FIELD admin_only ON doc COMPUTED fn::r()
	PERMISSIONS FOR select WHERE $auth.admin = true;
DEFINE FIELD bad ON doc COMPUTED fn::r() PERMISSIONS FOR select NONE;
DEFINE FUNCTION fn::boom() { THROW 'boom'; };
DEFINE FIELD gated ON gate COMPUTED fn::boom()
	PERMISSIONS FOR select WHERE $auth.admin = true;
DEFINE FUNCTION OVERWRITE fn::r() { CREATE log SET n = 1; RETURN 1; };
