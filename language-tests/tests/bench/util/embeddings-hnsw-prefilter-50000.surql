/**
# 50000 8-dimensional embeddings with an HNSW index and a selectivity-matrix
# of filter columns for the KNN prefilter benches (#548):
#
#   column    kind          probe        matches  selectivity  default tier
#   cat1000   int (typed)   = 7          50       0.1%         exact
#   cat100    int (typed)   = 7          500      1%           exact
#   cat10     int (typed)   = 7          5000     10%          graph
#   flag90    bool (typed)  = true       45000    90%          graph
#   rawcat    (undeclared)  = 7          500      1%           exact tier, residual kept
#
# Every column's equality becomes an allow-list prefilter. A column whose
# index has recorded one entry per record answers it exactly and the conjunct
# leaves the in-traversal filter; any other column's allow-list is a superset,
# so the conjunct is also judged on each candidate. The typed columns hold a
# scalar in every row, so each of their indexes records one entry per record. One extra row,
# `embeddings:mixed`, stores an array in `rawcat` and matches no probe, so
# `rawcat`'s index never records one entry per record. `rawcat = 7` vs
# `cat100 = 7` is a same-dataset, same-graph A/B of that exactness
# (knn_hnsw_prefilter_residual_1pct.surql vs knn_hnsw_prefilter_sel01.surql).
#
# Vectors and categories are DETERMINISTIC (derived from the record index, no
# `rand`). Combined with `SURREAL_HNSW_BUILD_SEED`, the built graph is
# identical across runs. 50k rows (not 5k) so the selectivity matrix actually
# spans the exact/graph tier boundary (default T_exact = 2000). `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE embeddings SCHEMALESS;
	DEFINE FIELD cat1000 ON embeddings TYPE int;
	DEFINE FIELD cat100 ON embeddings TYPE int;
	DEFINE FIELD cat10 ON embeddings TYPE int;
	DEFINE FIELD flag90 ON embeddings TYPE bool;
	-- The b-tree indexes are defined BEFORE the inserts so entries are
	-- written synchronously per row through the document pipeline — an
	-- in-block DEFINE INDEX after the rows does not backfill them.
	DEFINE INDEX emb_cat1000 ON embeddings FIELDS cat1000;
	DEFINE INDEX emb_cat100 ON embeddings FIELDS cat100;
	DEFINE INDEX emb_cat10 ON embeddings FIELDS cat10;
	DEFINE INDEX emb_flag90 ON embeddings FIELDS flag90;
	DEFINE INDEX emb_rawcat ON embeddings FIELDS rawcat;
	DEFINE INDEX emb_hnsw ON embeddings FIELDS vec HNSW DIMENSION 8 DIST EUCLIDEAN TYPE F32 EFC 150 M 12;
	-- Chunked: one array::range(0, 50000) exceeds the 1 MiB function-output
	-- cap, so build 50 chunks of 1000. The record index is inlined as
	-- `$chunk * 1000 + $j` everywhere; the modular fields reduce to `$j % k`
	-- since every modulus divides the chunk size, preserving the intended
	-- per-value distributions.
	FOR $chunk IN array::range(0, 50) {
		FOR $j IN array::range(0, 1000) {
			CREATE type::record("embeddings", $chunk * 1000 + $j) SET
				vec = array::map(array::range(0, 8), |$k| ((($chunk * 1000 + $j) * 7 + $k * 131) % 1000) / 1000.0),
				cat1000 = $j % 1000,
				cat100 = $j % 100,
				cat10 = $j % 10,
				flag90 = $j % 10 != 0,
				rawcat = $j % 100
			RETURN NONE;
		};
	};
	CREATE embeddings:mixed SET
		vec = [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
		cat1000 = -1,
		cat100 = -1,
		cat10 = -1,
		flag90 = false,
		rawcat = [-1, -2]
	RETURN NONE;
	RETURN 'OK';
}
