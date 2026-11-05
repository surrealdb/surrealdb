/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A B-tree key spelled a decimal with its trailing zeros, so `1.25dec * 2` (stored as 2.50) and the literal `2.5dec` were equal values at two keys: an indexed equality or range returned fewer rows than the same read without the index, a UNIQUE index admitted both, and a KNN allow-list built from an index equality lost the record holding the computed decimal. An index defined on this build respells every decimal without trailing zeros, in the values it stores and in the bounds it reads with, so every read below answers what its WITH NOINDEX twin answers. The pair of values compares equal but is stored at two scales, which type::is_decimal alone cannot show, so the record's own spelling is asserted as text."""
issue = 1336

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 2.50dec, id: k:1 }]"

[[test.results]]
value = "[{ c: 2.5dec, id: k:2 }]"

[[test.results]]
value = "[{ c: 2.6dec, id: k:3 }]"

[[test.results]]
value = "[{ c: 2.4dec, id: k:4 }]"

[[test.results]]
value = "[true, true, true, true]"

[[test.results]]
value = "['2.50', '2.5']"

[[test.results]]
value = "[k:1, k:2]"

[[test.results]]
value = "[k:1, k:2]"

[[test.results]]
value = "[k:1, k:2]"

[[test.results]]
value = "[k:1, k:2, k:3]"

[[test.results]]
value = "[k:1, k:2, k:3]"

[[test.results]]
value = "[k:1, k:2, k:4]"

[[test.results]]
value = "[k:1, k:2, k:4]"

[[test.results]]
value = "[k:3]"

[[test.results]]
value = "[k:3]"

[[test.results]]
value = "[k:4]"

[[test.results]]
value = "[k:4]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: k, index: k_c, access: = 2.5dec, direction: Forward]
''''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 2.50dec, id: u:1 }]"

[[test.results]]
error = "Database index `u_c` already contains 2.5dec, with record `u:1`"

[[test.results]]
value = "[{ c: 3dec, id: u:3 }]"

[[test.results]]
error = "Database index `u_c` already contains 2.50dec, with record `u:1`"

[[test.results]]
value = "[u:1]"

[[test.results]]
value = "[u:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 2.50dec, id: kn:1, vec: [0f, 0f] }]"

[[test.results]]
value = "[{ c: 2.5dec, id: kn:2, vec: [1f, 1f] }]"

[[test.results]]
value = "[{ c: 9dec, id: kn:3, vec: [2f, 2f] }]"

[[test.results]]
value = "[kn:1, kn:2]"

[[test.results]]
value = "[kn:1, kn:2]"

*/
DEFINE TABLE k SCHEMALESS;
DEFINE INDEX k_c ON k FIELDS c;
CREATE k:1 SET c = 1.25dec * 2;
CREATE k:2 SET c = 2.5dec;
CREATE k:3 SET c = 2.6dec;
CREATE k:4 SET c = 2.4dec;

-- Every value is a decimal, and the computed one keeps its trailing zero.
SELECT VALUE type::is_decimal(c) FROM k ORDER BY id;
SELECT VALUE <string> c FROM k:1, k:2;

-- Equality, either spelling of the probe.
SELECT VALUE id FROM k WITH INDEX k_c WHERE c = 2.5dec ORDER BY id;
SELECT VALUE id FROM k WITH NOINDEX WHERE c = 2.5dec ORDER BY id;
SELECT VALUE id FROM k WITH INDEX k_c WHERE c = 1.25dec * 2 ORDER BY id;

-- Inclusive and exclusive bounds.
SELECT VALUE id FROM k WITH INDEX k_c WHERE c >= 2.5dec ORDER BY id;
SELECT VALUE id FROM k WITH NOINDEX WHERE c >= 2.5dec ORDER BY id;
SELECT VALUE id FROM k WITH INDEX k_c WHERE c <= 2.5dec ORDER BY id;
SELECT VALUE id FROM k WITH NOINDEX WHERE c <= 2.5dec ORDER BY id;
SELECT VALUE id FROM k WITH INDEX k_c WHERE c > 2.5dec ORDER BY id;
SELECT VALUE id FROM k WITH NOINDEX WHERE c > 2.5dec ORDER BY id;
SELECT VALUE id FROM k WITH INDEX k_c WHERE c < 2.5dec ORDER BY id;
SELECT VALUE id FROM k WITH NOINDEX WHERE c < 2.5dec ORDER BY id;

-- The equality is answered from the index.
EXPLAIN SELECT VALUE id FROM k WHERE c = 2.5dec;

-- A UNIQUE index holds one record per value, whichever way it is spelled.
DEFINE TABLE u SCHEMALESS;
DEFINE INDEX u_c ON u FIELDS c UNIQUE;
CREATE u:1 SET c = 1.25dec * 2;
CREATE u:2 SET c = 2.5dec;
CREATE u:3 SET c = 3dec;
UPDATE u:3 SET c = 1.25dec * 2;
SELECT VALUE id FROM u WITH INDEX u_c WHERE c = 2.5dec;
SELECT VALUE id FROM u WITH INDEX u_c WHERE c >= 2.5dec AND c <= 2.5dec;

-- A KNN search restricted to the records an index equality names keeps both,
-- as the unindexed read of the equality does.
DEFINE TABLE kn SCHEMALESS;
DEFINE INDEX kn_c ON kn FIELDS c;
DEFINE INDEX kn_vec ON kn FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN;
CREATE kn:1 SET c = 1.25dec * 2, vec = [0.0, 0.0];
CREATE kn:2 SET c = 2.5dec, vec = [1.0, 1.0];
CREATE kn:3 SET c = 9dec, vec = [2.0, 2.0];
SELECT VALUE id FROM kn WHERE c = 2.5dec AND vec <|4,40|> [0.0, 0.0];
SELECT VALUE id FROM kn WITH NOINDEX WHERE c = 2.5dec ORDER BY id;
