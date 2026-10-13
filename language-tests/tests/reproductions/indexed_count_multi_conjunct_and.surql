/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Index-backed COUNT/keys-only scans must answer the whole WHERE clause: the legacy compute-only engine counted a single index branch and dropped the other conjuncts (AND), or summed overlapping branches (OR)"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 10 }]"

[[test.results]]
value = "[{ count: 10 }]"

[[test.results]]
value = "[{ count: 40 }]"

[[test.results]]
value = "[{ count: 15 }]"

[[test.results]]
value = "[{ count: 10 }]"

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = "[{ count: 10 }]"

[[test.results]]
value = "10"

[[test.results]]
value = "[{ count: 10 }]"

[[test.results]]
value = "[{ count: 15 }]"
*/

DEFINE INDEX idx_category ON items FIELDS category;
DEFINE INDEX idx_flag ON items FIELDS flag;
DEFINE INDEX idx_num ON items FIELDS num;
DEFINE INDEX idx_cat_num ON items FIELDS category, num;
FOR $i IN 0..60 {
    CREATE type::record('items', $i) SET category = ['red', 'green', 'blue'][$i % 3], flag = $i % 2 == 0, num = $i;
};
-- AND of two single-column indexes: the scan of one index must not drop the other conjunct
SELECT count() FROM items WHERE category = 'red' AND flag = true GROUP ALL;
-- Same query without indexes as the reference result
SELECT count() FROM items WITH NOINDEX WHERE category = 'red' AND flag = true GROUP ALL;
-- OR union with overlap (red:20 + flag:30 - overlap:10): overlapping branches must be deduplicated
SELECT count() FROM items WHERE category = 'red' OR flag = true GROUP ALL;
-- Range scan + residual equality conjunct
SELECT count() FROM items WHERE num > 29 AND flag = true GROUP ALL;
-- Compound index (category, num) fully covering the clause (count fast path stays correct)
SELECT count() FROM items WHERE category = 'red' AND num > 29 GROUP ALL;
-- Compound index + residual conjunct
SELECT count() FROM items WHERE category = 'red' AND num > 29 AND flag = true GROUP ALL;
-- Repeated conjunct
SELECT count() FROM items WHERE flag = true AND flag = true AND category = 'red' GROUP ALL;
-- count() without GROUP ALL (keys-only iteration): one row per matching record
RETURN array::len(SELECT count() FROM items WHERE category = 'red' AND flag = true);
-- Two range conjuncts merged on the same index (count fast path stays correct)
SELECT count() FROM items WHERE num > 29 AND num < 40 GROUP ALL;
-- AND nested inside OR
SELECT count() FROM items WHERE (category = 'red' AND flag = true) OR num > 54 GROUP ALL;
