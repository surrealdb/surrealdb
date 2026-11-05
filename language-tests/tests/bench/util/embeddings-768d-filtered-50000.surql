/**
# 50000 768-dimensional clustered embeddings shaped like crud-bench's filtered
# vector corpus (`config/vector-filtered.toml`), for the filtered-KNN benches in
# `bench/vector/knn_*_768_filtered_*.surql`.
#
# Two tables carry identical rows: `record` has the HNSW index and `record_dk`
# the DiskANN index, each with crud-bench's DDL parameters. They are separate
# tables because two vector indexes on one field leave the planner to pick
# between them, and a second embedding field would double the record the
# benches exist to measure decoding.
#
# Every row carries each filter column twice, so one dataset and one graph
# serve both of crud-bench's variants:
#
#   column       declared         indexed  crud-bench variant
#   number       no (schemaless)  no       default — predicate tested per candidate
#   status       no (schemaless)  no       default
#   number_idx   TYPE int         b-tree   `filter_index = true` — KNN pre-filter
#   status_idx   TYPE string      b-tree   `filter_index = true`
#
#   predicate          matches  selectivity  pre-filter tier (T_exact = 2000)
#   number <= 50       500      1%           exact
#   number <= 500      5000     10%          graph
#   status = 'published' 16667  33%          graph
#
# 50k rows is crud-bench's filtered scale, and it puts the 1% range on the
# exact tier and the 10% range and the tag on the graph tier, as there. The
# per-candidate cost the unindexed benches measure is set by the 768-d record,
# not the row count: fetching a candidate decodes its 768-float embedding,
# which the 8-d prefilter benches cannot show.
#
# The vector indexes carry crud-bench's parameters: HNSW M 16 EFC 200, DiskANN
# DEGREE 64 L_BUILD 100 ALPHA 1.2, both cosine.
#
# `number` is ((i * 7919) % 5000) + 1: a permutation of 1..5000 over every 5000
# consecutive rows, so each value occurs exactly 10 times and the counts above
# are exact. `status` cycles with i % 3.
#
# Vectors follow crud-bench's `vector:768:clustered:200`: unit-length
# centroids, each row its centroid plus a noise vector of length 0.35. Drawing
# 768 fresh components per row costs 38.4M closure calls, so rows draw their
# noise from a bank of 1009 vectors instead. Row i takes centroid i % 199 and
# noise i % 1009 — 199 centroids rather than 200 because both counts are prime
# and coprime with 5000 and 3, so the (centroid, noise) pair is unique per row
# and independent of the filter columns, and no two rows share a vector. `$q`
# is centroid 17 plus a noise vector outside the bank, so it is not a stored
# row; at 1% only a couple of the matching rows share its cluster, which is the
# shape that makes a filtered graph walk visit many candidates.
#
# Deterministic: the bench harness reseeds the engine RNG before every dataset
# build and pins `SURREAL_HNSW_BUILD_SEED`, so the rows and the HNSW graph are
# identical across runs. The vector indexes are defined before the rows, so the
# harness's post-import compaction drain builds both graphs before the first
# measured iteration. `run = false`.

[test]
run = false
*/

DEFINE TABLE record SCHEMALESS;
DEFINE FIELD number_idx ON record TYPE int;
DEFINE FIELD status_idx ON record TYPE string;
DEFINE INDEX record_number_idx ON record FIELDS number_idx;
DEFINE INDEX record_status_idx ON record FIELDS status_idx;
DEFINE INDEX record_hnsw ON record FIELDS embedding HNSW DIMENSION 768 DIST COSINE EFC 200 M 16;

DEFINE TABLE record_dk SCHEMALESS;
DEFINE FIELD number_idx ON record_dk TYPE int;
DEFINE INDEX record_dk_number_idx ON record_dk FIELDS number_idx;
DEFINE INDEX record_dk_diskann ON record_dk FIELDS embedding DISKANN DIMENSION 768 DIST COSINE DEGREE 64 L_BUILD 100 ALPHA 1.2;

-- Rows are built with `array::map` in a `LET` and written with one `INSERT`
-- per batch: a closure evaluated inside a `CREATE` or `INSERT` expression is
-- an order of magnitude slower than the same closure in a `LET`.
LET $centroids = array::map(array::range(0, 199), |$c| { id: $c, v: vector::normalize(array::map(array::range(0, 768), |$k| rand::float(-1.0, 1.0))) });
INSERT INTO centroid $centroids RETURN NONE;
LET $noise = array::map(array::range(0, 1009), |$n| { id: $n, v: vector::scale(vector::normalize(array::map(array::range(0, 768), |$k| rand::float(-1.0, 1.0))), 0.35) });
INSERT INTO noise $noise RETURN NONE;

DEFINE PARAM $q VALUE vector::add(centroid:17.v, vector::scale(vector::normalize(array::map(array::range(0, 768), |$k| rand::float(-1.0, 1.0))), 0.35));

-- One top-level statement per 1000-row chunk, so each chunk commits in its
-- own transaction: the cost of a write grows with the size of the transaction
-- it lands in, so writing all 100k rows in one transaction is quadratic.
-- Chunks also keep each `array::range` under the 1 MiB generation cap.
DEFINE FUNCTION fn::chunk($chunk: int) {
	LET $rows = array::map(array::range($chunk * 1000, $chunk * 1000 + 1000), |$i| {
		id: $i,
		embedding: vector::add(type::record('centroid', $i % 199).v, type::record('noise', $i % 1009).v),
		number: (($i * 7919) % 5000) + 1,
		number_idx: (($i * 7919) % 5000) + 1,
		status: ['draft', 'published', 'archived'][$i % 3],
		status_idx: ['draft', 'published', 'archived'][$i % 3],
		text: rand::string(50),
		description: rand::string(250),
	});
	INSERT INTO record $rows RETURN NONE;
	INSERT INTO record_dk $rows RETURN NONE;
};
fn::chunk(0); fn::chunk(1); fn::chunk(2); fn::chunk(3); fn::chunk(4); fn::chunk(5); fn::chunk(6); fn::chunk(7); fn::chunk(8); fn::chunk(9);
fn::chunk(10); fn::chunk(11); fn::chunk(12); fn::chunk(13); fn::chunk(14); fn::chunk(15); fn::chunk(16); fn::chunk(17); fn::chunk(18); fn::chunk(19);
fn::chunk(20); fn::chunk(21); fn::chunk(22); fn::chunk(23); fn::chunk(24); fn::chunk(25); fn::chunk(26); fn::chunk(27); fn::chunk(28); fn::chunk(29);
fn::chunk(30); fn::chunk(31); fn::chunk(32); fn::chunk(33); fn::chunk(34); fn::chunk(35); fn::chunk(36); fn::chunk(37); fn::chunk(38); fn::chunk(39);
fn::chunk(40); fn::chunk(41); fn::chunk(42); fn::chunk(43); fn::chunk(44); fn::chunk(45); fn::chunk(46); fn::chunk(47); fn::chunk(48); fn::chunk(49);
REMOVE FUNCTION fn::chunk;
