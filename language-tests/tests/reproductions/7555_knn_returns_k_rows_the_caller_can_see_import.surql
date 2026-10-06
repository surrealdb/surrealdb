/**
[test]
reason = "Fixture for 7555_knn_returns_k_rows_the_caller_can_see_streaming.surql and _legacy.surql: an HNSW-indexed and a DiskANN-indexed table whose SELECT permission hides every row nearer the query than alice's, plus the record user those tests sign in as. Runs as owner."

[[test.results]]
value = "'OK'"

*/
-- Rows 1..20 are bob's and lie nearest the origin; rows 21..30 are alice's
-- and lie beyond them, so every one of the table's k nearest neighbours to
-- [0] is hidden from alice for any k <= 20.
{
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
		DURATION FOR TOKEN 15m, FOR SESSION 6h;
	CREATE user:alice SET email = 'alice@surrealdb.com', pass = crypto::argon2::generate('secret');
	CREATE user:bob SET email = 'bob@surrealdb.com', pass = crypto::argon2::generate('secret');

	DEFINE TABLE hnsw_chunk SCHEMALESS PERMISSIONS
		FOR select WHERE owner = $auth
		FOR create, update, delete NONE;
	DEFINE INDEX hnsw_chunk_emb ON hnsw_chunk FIELDS emb HNSW DIMENSION 1 DIST EUCLIDEAN TYPE F32;

	DEFINE TABLE diskann_chunk SCHEMALESS PERMISSIONS
		FOR select WHERE owner = $auth
		FOR create, update, delete NONE;
	DEFINE INDEX diskann_chunk_emb ON diskann_chunk FIELDS emb DISKANN DIMENSION 1 DIST EUCLIDEAN TYPE F32;

	FOR $i IN 1..=30 {
		LET $owner = IF $i > 20 { user:alice } ELSE { user:bob };
		CREATE type::record('hnsw_chunk', $i) SET emb = [<float> $i], owner = $owner;
		CREATE type::record('diskann_chunk', $i) SET emb = [<float> $i], owner = $owner;
	};

	RETURN 'OK';
}
