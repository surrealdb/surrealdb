/**
[test]
requires = ["diskann"]
reason = "DISKANN measures TYPE I8 and U8 vectors with kernels that accumulate in i32, which a vector longer than 33025 components (255² = 65025 per component) can overflow. Those types are refused past that DIMENSION, CONCURRENTLY or not, before the definition commits; floating-point types are not limited by it."

[[test.results]]
value = "NONE"

[[test.results]]
error = "DISKANN TYPE I8 and U8 support DIMENSION up to 33025, got 33026"

[[test.results]]
error = "DISKANN TYPE I8 and U8 support DIMENSION up to 33025, got 33026"

[[test.results]]
error = "DISKANN TYPE I8 and U8 support DIMENSION up to 33025, got 33026"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{  }"

[[test.results]]
value = "{  }"

[[test.results]]
value = "['at_limit_u8']"

*/

DEFINE INDEX at_limit_u8 ON t1 FIELDS v DISKANN DIMENSION 33025 DIST EUCLIDEAN TYPE U8;
DEFINE INDEX past_limit_u8 ON t2 FIELDS v DISKANN DIMENSION 33026 DIST EUCLIDEAN TYPE U8;
DEFINE INDEX past_limit_i8 ON t3 FIELDS v DISKANN DIMENSION 33026 DIST INNER_PRODUCT TYPE I8;
DEFINE INDEX past_limit_concurrently ON t5 FIELDS v DISKANN DIMENSION 33026 DIST EUCLIDEAN TYPE U8 CONCURRENTLY;
DEFINE INDEX past_limit_f32 ON t4 FIELDS v DISKANN DIMENSION 33026 DIST EUCLIDEAN TYPE F32;
(INFO FOR TABLE t2).indexes;
(INFO FOR TABLE t5).indexes;
(INFO FOR TABLE t1).indexes.keys();
