/**
[test]

[[test.results]]
value = "'OK'"

*/
-- Fixture for knn_select_permission.surql: two HNSW-indexed tables whose
-- SELECT permission must be applied per candidate inside the ANN search,
-- plus the record user the test signs in as. Runs as owner.
{
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
		DURATION FOR TOKEN 15m, FOR SESSION 6h;
	CREATE user:alice SET email = 'alice@surrealdb.com', pass = crypto::argon2::generate('secret');
	CREATE user:bob SET email = 'bob@surrealdb.com', pass = crypto::argon2::generate('secret');

	-- Table permission on `owner`: candidates owned by someone else must be
	-- invisible to the residual WHERE and must not consume top-K slots.
	DEFINE TABLE vec SCHEMALESS PERMISSIONS
		FOR select WHERE owner = $auth
		FOR create, update, delete NONE;
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

	-- Table permission referencing `id`: the per-candidate check inside the
	-- ANN search must see the record id spliced into the candidate document.
	DEFINE TABLE vecb SCHEMALESS PERMISSIONS
		FOR select WHERE owner = $auth AND id != vecb:1
		FOR create, update, delete NONE;
	DEFINE INDEX vecb_pt ON vecb FIELDS point HNSW DIMENSION 1 DIST EUCLIDEAN TYPE F32;
	INSERT INTO vecb [
		{ id: vecb:1, point: [ 10f ], owner: user:alice, flag: true },
		{ id: vecb:2, point: [ 20f ], owner: user:alice, flag: true },
		{ id: vecb:3, point: [ 30f ], owner: user:alice, flag: false },
		{ id: vecb:4, point: [ 40f ], owner: user:alice, flag: true },
		{ id: vecb:5, point: [ 50f ], owner: user:alice, flag: true },
		{ id: vecb:6, point: [ 60f ], owner: user:alice, flag: true }
	];

	RETURN 'OK';
}
