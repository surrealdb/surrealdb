/**
# 5000 8-dimensional embeddings with a DISKANN index and both a typed,
# indexed `cat100` column (1% selectivity, exactly bitmap-coverable — its
# equality becomes a #548 allow-list prefilter) and an undeclared `rawcat`
# twin (in-traversal residual baseline). Vectors and categories are
# DETERMINISTIC (derived from the record index). `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE embeddings SCHEMALESS;
	DEFINE FIELD cat100 ON embeddings TYPE int;
	-- Indexes are defined BEFORE the inserts so entries are written
	-- synchronously per row — an in-block DEFINE INDEX after the rows does
	-- not backfill them.
	DEFINE INDEX emb_cat100 ON embeddings FIELDS cat100;
	DEFINE INDEX emb_rawcat ON embeddings FIELDS rawcat;
	DEFINE INDEX emb_diskann ON embeddings FIELDS vec DISKANN DIMENSION 8 DIST EUCLIDEAN TYPE F32;
	FOR $i IN array::range(0, 5000) {
		CREATE type::record("embeddings", $i) SET
			vec = array::map(array::range(0, 8), |$j| (($i * 7 + $j * 131) % 1000) / 1000.0),
			cat100 = $i % 100,
			rawcat = $i % 100
		RETURN NONE;
	};
	RETURN 'OK';
}
