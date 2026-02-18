/**
[test]
reason = "Import: setup tables with field and table permissions for FETCH bypass test"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ email: 'alice@example.com', id: user:alice, password: 'hashed_pw_alice' }]"

[[test.results]]
value = "[{ email: 'bob@example.com', id: user:bob, password: 'hashed_pw_bob' }]"

[[test.results]]
value = "[{ id: doc:1, owner: user:alice }]"

[[test.results]]
value = "[{ id: doc:2, owner: user:bob }]"
*/

DEFINE ACCESS test ON DATABASE TYPE RECORD;
DEFINE TABLE user PERMISSIONS FOR SELECT WHERE id = $auth;
DEFINE FIELD email ON user;
DEFINE FIELD password ON user PERMISSIONS FOR SELECT NONE;
DEFINE TABLE doc PERMISSIONS FOR SELECT FULL;
DEFINE FIELD owner ON doc TYPE record<user>;
CREATE user:alice SET email = 'alice@example.com', password = 'hashed_pw_alice';
CREATE user:bob SET email = 'bob@example.com', password = 'hashed_pw_bob';
CREATE doc:1 SET owner = user:alice;
CREATE doc:2 SET owner = user:bob;
