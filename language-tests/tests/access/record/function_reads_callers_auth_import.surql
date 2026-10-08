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
value = "[{ id: doc:1, owner: user:alice }]"

[[test.results]]
value = "[{ id: doc:2, owner: user:bob }]"

[[test.results]]
value = "NONE"

*/
DEFINE ACCESS account ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('user', $user) )
	DURATION FOR TOKEN 18m, FOR SESSION 12h;

DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE doc SCHEMALESS PERMISSIONS FULL;

CREATE user:alice, user:bob;
CREATE doc:1 SET owner = user:alice;
CREATE doc:2 SET owner = user:bob;

DEFINE FUNCTION fn::mine() { RETURN SELECT VALUE id FROM doc WHERE owner = $auth; };
