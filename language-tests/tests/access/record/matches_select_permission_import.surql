/**
[test]

[[test.results]]
value = "'OK'"

*/
-- Fixture for matches_select_permission.surql: a full-text indexed table whose
-- SELECT permission depends on the record's owner, a vector indexed one under
-- the same permission, one that denies every read, and the record user the
-- test signs in as. Runs as owner.
{
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
		DURATION FOR TOKEN 15m, FOR SESSION 6h;
	CREATE user:alice SET email = 'alice@surrealdb.com', pass = crypto::argon2::generate('secret');
	CREATE user:bob SET email = 'bob@surrealdb.com', pass = crypto::argon2::generate('secret');

	DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase,ascii;

	DEFINE TABLE note SCHEMALESS PERMISSIONS
		FOR select WHERE owner = $auth
		FOR create, update, delete NONE;
	DEFINE FIELD secret ON note PERMISSIONS FOR select NONE;
	DEFINE INDEX note_text ON note FIELDS text FULLTEXT ANALYZER simple BM25(1.2, 0.75) HIGHLIGHTS;
	DEFINE INDEX note_vec ON note FIELDS point HNSW DIMENSION 1 DIST EUCLIDEAN TYPE F32;
	INSERT INTO note [
		{ id: note:1, text: 'red apple pie', owner: user:alice, point: [ 10f ], secret: 's' },
		{ id: note:2, text: 'red apple tart', owner: user:bob, point: [ 20f ], secret: 's' },
		{ id: note:3, text: 'green apple pie', owner: user:alice, point: [ 30f ], secret: 's' },
		{ id: note:4, text: 'red cherry pie', owner: user:bob, point: [ 40f ], secret: 's' },
		{ id: note:5, text: 'red apple crumble', owner: user:alice, point: [ 50f ], secret: 's' },
		{ id: note:6, text: 'blue plum tart', owner: user:bob, point: [ 60f ], secret: 's' }
	];

	DEFINE TABLE sealed SCHEMALESS PERMISSIONS FOR select NONE;
	DEFINE INDEX sealed_text ON sealed FIELDS text FULLTEXT ANALYZER simple BM25(1.2, 0.75) HIGHLIGHTS;
	INSERT INTO sealed [
		{ id: sealed:1, text: 'red apple pie' },
		{ id: sealed:2, text: 'red apple tart' }
	];

	RETURN 'OK';
}
