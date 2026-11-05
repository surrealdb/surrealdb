/**
[test]
reason = "Setup for the tests that an API's PERMISSIONS clause, handler and middleware read tables the invoking record user cannot read: a ban list and a secret table no record user may select."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid )
	DURATION FOR SESSION 24h;

DEFINE CONFIG API PERMISSIONS FULL;

DEFINE TABLE user SCHEMALESS PERMISSIONS NONE;
DEFINE TABLE ban SCHEMALESS PERMISSIONS NONE;
DEFINE TABLE secret SCHEMALESS PERMISSIONS NONE;

CREATE user:banned, user:member;
CREATE ban:1 SET user = user:banned;
CREATE secret:1 SET v = 1;
CREATE secret:2 SET v = 2;

DEFINE FUNCTION fn::with_secret($req: object, $next: function) -> object {
	LET $res = $next($req);
	RETURN $res + { headers: { 'x-secret': <string> (SELECT VALUE v FROM secret ORDER BY v) } };
};

DEFINE API "/members"
	FOR get
		PERMISSIONS WHERE (SELECT * FROM ban WHERE user = $auth.id).len() = 0
		THEN { RETURN { status: 200, body: 'in' } };

DEFINE API "/members_fn"
	FOR get
		PERMISSIONS WHERE fn::not_banned()
		THEN { RETURN { status: 200, body: 'in' } };

DEFINE FUNCTION fn::not_banned() { RETURN count(SELECT * FROM ban WHERE user = $auth.id) = 0 };

DEFINE API "/secret"
	FOR get
		PERMISSIONS FULL
		THEN { status: 200, body: (SELECT VALUE v FROM secret ORDER BY v) };

DEFINE API "/secret_block"
	FOR get
		PERMISSIONS FULL
		THEN { RETURN { status: 200, body: (SELECT VALUE v FROM secret ORDER BY v) } };

DEFINE API "/secret_middleware"
	FOR get
		MIDDLEWARE fn::with_secret()
		PERMISSIONS FULL
		THEN { status: 200, body: 'ok' };
