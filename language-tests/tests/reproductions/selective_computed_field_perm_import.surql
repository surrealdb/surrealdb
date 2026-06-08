/**
[test]
reason = "Setup: tables with a computed field `flag` that drives a field permission on `secret`. Record users querying `SELECT secret FROM t` (selective projection) must still evaluate `flag` so the permission check is correct."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid );

DEFINE TABLE notes SCHEMAFULL
	PERMISSIONS
		FOR select FULL
		FOR create FULL
		FOR update, delete NONE;

-- `flag` is a computed field derived from `score`.
DEFINE FIELD score ON notes TYPE int;
DEFINE FIELD flag ON notes TYPE bool COMPUTED score >= 10;

-- `secret` is gated by `flag`. A selective projection like `SELECT secret`
-- must still compute `flag` before the permission is evaluated, otherwise
-- the permission decision is made against a row missing `flag`.
DEFINE FIELD secret ON notes TYPE string
	PERMISSIONS FOR select WHERE flag;

CREATE notes:high SET score = 50, secret = 'visible_when_high';
CREATE notes:low SET score = 1, secret = 'must_be_hidden';
