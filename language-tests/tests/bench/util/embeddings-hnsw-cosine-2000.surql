/**
# 2000 768-dimensional random embeddings with a cosine HNSW index, for indexed
# nearest-neighbour benches. A realistic embedding dimension (768) is used so
# the per-distance magnitude cost is representative.
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
	CREATE |embeddings:2000| SET vec = array::map(array::range(0, 768), |$i| rand::float(0.0, 1.0)) RETURN NONE;
	RETURN 'OK';
};

DEFINE INDEX emb_hnsw ON embeddings FIELDS vec HNSW DIMENSION 768 DIST COSINE TYPE F32 EFC 200 M 12;
