/**
[test]

[[test.results]]
value = "'OK'"

*/
-- Fixture for knn_prefilter_select_permission.surql: an HNSW-indexed,
-- permission-restricted table whose `flag` column is typed and indexed so a
-- flag equality is exactly bitmap-coverable (#548). The allow-list is built
-- from index entries and therefore contains hidden (bob's) rows too; the
-- per-record SELECT permission must still exclude them from the results
-- without letting them consume top-K slots. Runs as owner.
{
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
		DURATION FOR TOKEN 15m, FOR SESSION 6h;
	CREATE user:alice SET email = 'alice@surrealdb.com', pass = crypto::argon2::generate('secret');
	CREATE user:bob SET email = 'bob@surrealdb.com', pass = crypto::argon2::generate('secret');

	DEFINE TABLE vec SCHEMALESS PERMISSIONS
		FOR select WHERE owner = $auth
		FOR create, update, delete NONE;
	DEFINE FIELD flag ON vec TYPE bool;
	DEFINE INDEX vec_flag ON vec FIELDS flag;
	DEFINE INDEX vec_pt ON vec FIELDS point HNSW DIMENSION 1 DIST EUCLIDEAN TYPE F32;
	INSERT INTO vec [
		{ id: vec:1,  point: [ 10f ],  owner: user:alice, flag: true },
		{ id: vec:2,  point: [ 20f ],  owner: user:bob,   flag: true },
		{ id: vec:3,  point: [ 30f ],  owner: user:alice, flag: false },
		{ id: vec:4,  point: [ 40f ],  owner: user:bob,   flag: true },
		{ id: vec:5,  point: [ 50f ],  owner: user:alice, flag: true },
		{ id: vec:6,  point: [ 60f ],  owner: user:bob,   flag: true },
		{ id: vec:7,  point: [ 70f ],  owner: user:alice, flag: false },
		{ id: vec:8,  point: [ 80f ],  owner: user:bob,   flag: true },
		{ id: vec:9,  point: [ 90f ],  owner: user:alice, flag: true },
		{ id: vec:10, point: [ 100f ], owner: user:bob,   flag: true },
		{ id: vec:11, point: [ 110f ], owner: user:alice, flag: false },
		{ id: vec:12, point: [ 120f ], owner: user:bob,   flag: true }
	];

	RETURN 'OK';
}
