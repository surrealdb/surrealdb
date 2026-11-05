/**
[test]
reason = "Fixture for a_knn_search_gates_candidates_on_a_permission_reading_a_computed_field.surql: HNSW- and DiskANN-indexed tables whose SELECT permission reads a COMPUTED field and hides every row nearer the query than those it admits, plus the record user the test signs in as. Runs as owner."

[[test.results]]
value = "'OK'"

*/
-- Rows 1..20 compute a `dbl` of at most 40, so the permission hides them, and
-- they lie nearest the origin; every one of the table's k nearest neighbours
-- to [0] is hidden for any k <= 20.
{
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
		DURATION FOR TOKEN 15m, FOR SESSION 6h;
	CREATE user:alice SET email = 'alice@surrealdb.com', pass = crypto::argon2::generate('secret');

	DEFINE TABLE hnsw_c SCHEMALESS PERMISSIONS
		FOR select WHERE dbl > 40
		FOR create, update, delete NONE;
	DEFINE FIELD dbl ON hnsw_c COMPUTED n * 2;
	DEFINE INDEX hnsw_c_emb ON hnsw_c FIELDS emb HNSW DIMENSION 1 DIST EUCLIDEAN TYPE F32;

	DEFINE TABLE diskann_c SCHEMALESS PERMISSIONS
		FOR select WHERE dbl > 40
		FOR create, update, delete NONE;
	DEFINE FIELD dbl ON diskann_c COMPUTED n * 2;
	DEFINE INDEX diskann_c_emb ON diskann_c FIELDS emb DISKANN DIMENSION 1 DIST EUCLIDEAN TYPE F32;

	FOR $i IN 1..=30 {
		CREATE type::record('hnsw_c', $i) SET emb = [<float> $i], n = $i;
		CREATE type::record('diskann_c', $i) SET emb = [<float> $i], n = $i;
	};

	RETURN 'OK';
}
