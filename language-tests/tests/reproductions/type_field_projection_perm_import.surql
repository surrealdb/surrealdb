/**
[test]
reason = "Setup: a permission expression that reads a computed field via the `type::field(...)` projection function. Static dependency analysis must recognise the projection and pre-compute `flag` before the permission is evaluated."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid );

DEFINE TABLE notes SCHEMAFULL
	PERMISSIONS
		FOR select FULL
		FOR create FULL
		FOR update, delete NONE;

DEFINE FIELD score ON notes TYPE int;
DEFINE FIELD flag ON notes TYPE bool COMPUTED score >= 10;

-- Permission gates `secret` behind `flag`, but references `flag` through
-- the `type::field` projection function rather than a bare idiom. The
-- dependency extractor must classify `type::field` as a projection and
-- recover `flag` from its literal argument, so a `SELECT VALUE secret`
-- still pre-computes `flag`.
DEFINE FIELD secret ON notes TYPE string
	PERMISSIONS FOR select WHERE type::field("flag");

CREATE notes:high SET score = 50, secret = 'visible_when_high';
CREATE notes:low SET score = 1, secret = 'must_be_hidden';
