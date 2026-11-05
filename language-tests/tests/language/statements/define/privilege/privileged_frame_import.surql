/**
[test]
reason = "Setup for the tests that code a definer wrote — a table or field permission, a field VALUE, ASSERT or DEFAULT, a computed field, an event, a closure in a permission — reads the session's own values and the database's parameters, and none of the caller's bindings."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE PARAM $allowed VALUE false PERMISSIONS FULL;
DEFINE PARAM $granted VALUE true PERMISSIONS FULL;

DEFINE FUNCTION fn::allowed() { RETURN $allowed } PERMISSIONS FULL;

DEFINE TABLE user SCHEMALESS PERMISSIONS NONE;
DEFINE TABLE direct SCHEMALESS PERMISSIONS FOR select WHERE $allowed;
DEFINE TABLE viafn SCHEMALESS PERMISSIONS FOR select WHERE fn::allowed();
DEFINE TABLE viaparam SCHEMALESS PERMISSIONS FOR select WHERE $granted;
DEFINE TABLE viaauth SCHEMALESS PERMISSIONS FOR select WHERE $auth.id = user:b;
DEFINE TABLE viaclosure SCHEMALESS PERMISSIONS FOR select WHERE array::any([1], |$x| $allowed);
DEFINE TABLE checked SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD guarded ON checked VALUE $allowed;
DEFINE FIELD asserted ON checked TYPE option<bool> ASSERT $allowed = false;
DEFINE FIELD computed ON checked COMPUTED $allowed;
DEFINE FIELD defaulted ON checked DEFAULT $allowed;
DEFINE TABLE evlog SCHEMALESS PERMISSIONS FULL;
DEFINE EVENT logged ON checked WHEN true THEN (CREATE evlog SET v = $allowed);

CREATE user:a, user:b;
CREATE direct:1 SET v = 1;
CREATE viafn:1 SET v = 1;
CREATE viaparam:1 SET v = 1;
CREATE viaauth:1 SET v = 1;
CREATE viaclosure:1 SET v = 1;
