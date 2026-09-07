/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: user:alice }, { id: user:bob }]"

[[test.results]]
value = "[{ id: grant:1, org: 'acme', owner: user:alice }]"

[[test.results]]
value = "[{ id: grant:2, org: 'globex', owner: user:bob }]"

[[test.results]]
value = "[{ id: doc:1, org: 'acme' }]"

[[test.results]]
value = "[{ id: doc:2, org: 'globex' }]"

*/
-- The CONTEXT clause is evaluated once, at authentication time, with $auth
-- already bound. Its whole result is stored on the session and read back as
-- $session.data, so a permission can filter on it without re-running the
-- lookup for every row.
DEFINE ACCESS account ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('user', $user) )
	CONTEXT ( SELECT VALUE org FROM grant WHERE owner = $auth )
	DURATION FOR TOKEN 18m, FOR SESSION 12h;

DEFINE TABLE grant SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth;
DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select WHERE org IN $session.data;

CREATE user:alice, user:bob;
CREATE grant:1 SET owner = user:alice, org = "acme";
CREATE grant:2 SET owner = user:bob, org = "globex";
CREATE doc:1 SET org = "acme";
CREATE doc:2 SET org = "globex";
