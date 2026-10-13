/**
# 1000 768-dimensional random embeddings WITHOUT an index. Used by the cosine
# index-build bench, which defines the index in the measured body so build time
# is what's timed. Kept at 1000 rows to bound the `rebuild = true` per-iteration
# reseed cost in CI; the norm-caching build speedup is already clear at this
# size. `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE embeddings SCHEMALESS;
	CREATE |embeddings:1000| SET vec = array::map(array::range(0, 768), |$i| rand::float(0.0, 1.0)) RETURN NONE;
	RETURN 'OK';
}
