/**
[test]
reason = """
	Setup for hidden_is_never_evaluated and hidden_is_never_evaluated_root: a
	table whose COMPUTED fields each draw the next value of one sequence, with
	select guards that deny the record user outright, deny it on a stored fact
	of the session, or leave the field visible, and a field whose body would
	raise if it ran. Runs as owner.

	Every record is created before any COMPUTED field is defined: a CREATE's
	output derives every computed field as the importing principal, which would
	draw the sequence and raise on `bad`. `fn::r` is read-only when `bad` is
	defined and is redefined with a write afterwards, so the definition-time
	check of register #17 accepts the field and any read that derives it raises.
	Nothing after the redefinition writes `doc`.
"""
run = false
*/

DEFINE SEQUENCE s;
DEFINE TABLE doc PERMISSIONS FULL;
DEFINE TABLE refs PERMISSIONS FULL;
DEFINE TABLE log;
CREATE doc:1;
CREATE refs:1 SET link = doc:1;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:reader SET admin = false;

DEFINE FIELD shown ON doc COMPUTED sequence::nextval('s');
DEFINE FIELD hidden ON doc COMPUTED sequence::nextval('s') PERMISSIONS FOR select NONE;
DEFINE FIELD admin_only ON doc COMPUTED sequence::nextval('s')
	PERMISSIONS FOR select WHERE $auth.admin = true;
DEFINE FUNCTION fn::r() { RETURN 1; };
DEFINE FIELD bad ON doc COMPUTED fn::r() PERMISSIONS FOR select NONE;
DEFINE FUNCTION OVERWRITE fn::r() { CREATE log SET n = 1; RETURN 1; };
