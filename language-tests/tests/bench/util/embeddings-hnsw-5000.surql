/**
# 5000 8-dimensional random embeddings with an HNSW index, for indexed
# nearest-neighbour benches.
#
# The rows are created in one statement and the index is defined in the next.
# They must stay separate statements: `DEFINE INDEX` backfills the index by
# reading the table through its own transaction, so rows written by the *same*
# transaction are invisible to it and the index would be built empty.
#
# `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"
*/

{
	DEFINE TABLE embeddings SCHEMALESS;
	CREATE |embeddings:5000| SET vec = array::map(array::range(0, 8), |$i| rand::float(0.0, 1.0)) RETURN NONE;
	RETURN 'OK';
};

DEFINE INDEX emb_hnsw ON embeddings FIELDS vec HNSW DIMENSION 8 DIST EUCLIDEAN TYPE F32 EFC 200 M 12;
