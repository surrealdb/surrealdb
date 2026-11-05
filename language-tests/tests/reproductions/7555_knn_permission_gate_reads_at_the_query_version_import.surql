/**
[env]
versioned = true

[test]
reason = "Fixture for 7555_knn_permission_gate_reads_at_the_query_version.surql: an HNSW-indexed table whose nearest row is alice's at a recorded timestamp and bob's afterwards, plus the record user that test signs in as. Runs as owner."
# Each statement must succeed; hashes and timestamps are not pinned.
results = [{ error = false }, { error = false }, { error = false }, { error = false }, { error = false }, { error = false }, { error = false }, { error = false }, { error = false }, { error = false }, { error = false }]

*/
DEFINE ACCESS account ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
	DURATION FOR TOKEN 15m, FOR SESSION 6h;
CREATE user:alice SET email = 'alice@surrealdb.com', pass = crypto::argon2::generate('secret');
CREATE user:bob SET email = 'bob@surrealdb.com', pass = crypto::argon2::generate('secret');

DEFINE TABLE vec SCHEMALESS PERMISSIONS
	FOR select WHERE owner = $auth
	FOR create, update, delete NONE;
DEFINE INDEX vec_pt ON vec FIELDS point HNSW DIMENSION 1 DIST EUCLIDEAN TYPE F32;
INSERT INTO vec [
	{ id: vec:1, point: [ 10f ], owner: user:alice },
	{ id: vec:2, point: [ 20f ], owner: user:bob },
	{ id: vec:3, point: [ 30f ], owner: user:alice },
	{ id: vec:4, point: [ 40f ], owner: user:bob },
	{ id: vec:5, point: [ 50f ], owner: user:alice },
	{ id: vec:6, point: [ 60f ], owner: user:bob }
];

-- Top-level statements, each its own transaction: the timestamp recorded
-- below falls after the fixture above commits and before the update
-- after it.
-- The instant the test reads at, readable by the record user.
DEFINE TABLE marker SCHEMALESS PERMISSIONS FOR select FULL;
SLEEP 10ms;
CREATE marker:ts SET at = time::now();
SLEEP 10ms;

-- After that instant alice's nearest row becomes bob's.
UPDATE vec:1 SET owner = user:bob;
