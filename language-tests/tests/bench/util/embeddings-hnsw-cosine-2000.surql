/**
# 2000 768-dimensional random embeddings with a cosine HNSW index, for indexed
# nearest-neighbour benches. A realistic embedding dimension (768) is used so
# the per-distance magnitude cost is representative. `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE embeddings SCHEMALESS;
	CREATE |embeddings:2000| SET vec = array::map(array::range(0, 768), |$i| rand::float(0.0, 1.0)) RETURN NONE;
	DEFINE INDEX emb_hnsw ON embeddings FIELDS vec HNSW DIMENSION 768 DIST COSINE TYPE F32 EFC 200 M 12;
	RETURN 'OK';
}
