/**
[test]
reason = "Setup: tables whose stored clauses invoke closures, and the records a record user reads through them."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE log SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE doc SCHEMALESS
	PERMISSIONS
		FOR select WHERE (|$o| $o = $auth.id)(owner)
			AND array::any(array::map([owner], |$w| $w = $auth.id)),
		FOR create FULL;
DEFINE FIELD x ON doc VALUE (|$v| $v * 10)($value);
DEFINE FIELD y ON doc ASSERT (|$v| $v > 0)($value);
DEFINE FIELD secret ON doc VALUE 's'
	PERMISSIONS FOR select WHERE array::all(array::map([owner], |$o| $o = $auth.id));
DEFINE EVENT ev ON doc WHEN $event = 'CREATE' THEN (
	CREATE log SET v = array::map([1], |$q| $q + $after.x)[0]
);

CREATE person:bob;
CREATE doc:1 SET owner = person:bob, x = 1, y = 1;
CREATE doc:2 SET owner = person:eve, x = 2, y = 1;
