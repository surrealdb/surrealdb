/**
# 20000 768-dimensional random embeddings WITHOUT an index. Used by the DiskANN
# index-build bench that defines the index in the measured body, so build time
# is what's timed.
#
# 20000 rows is 20x the 1000-row set, so the drain runs some twenty bounded
# compaction frames instead of one and the build is dominated by graph work
# rather than by the backfill scan — enough to exercise the compaction machinery
# at scale on a real backend while a `rebuild = true` bench, which re-imports
# the dataset before every measured iteration, stays affordable.
#
# It is still inside the 256 MiB `SURREAL_DISKANN_CACHE_SIZE`: the persisted
# graph costs roughly 3.6 KiB per row at this dimension (a 768-dim F32 element
# plus a DEGREE 64 adjacency list), so the cache stops holding it somewhere
# above 70000 rows. The cache-miss regime the epic is about is past that, and
# measuring it is what the `diag_diskann_build_rocksdb_200k` diagnostic is for —
# a build that size takes far too long to re-run per bench iteration.
#
# `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE embeddings SCHEMALESS;
	CREATE |embeddings:20000| SET vec = array::map(array::range(0, 768), |$i| rand::float(0.0, 1.0)) RETURN NONE;
	RETURN 'OK';
}
