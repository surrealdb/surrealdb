/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A membership subquery is lifted into a join only over a read-only probe side: the join reads the right relation once for every outer row, where the per-row form reads it after each row's writes"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, k: 1 }]"

[[test.results]]
value = "[{ id: t:2, k: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[r:2]"
*/
DEFINE TABLE r SCHEMALESS;
DEFINE TABLE t SCHEMALESS;
CREATE t:1 SET k = 1;
CREATE t:2 SET k = 2;

-- Leaves `r` holding exactly the key it was called with.
DEFINE FUNCTION fn::reset($k: int) {
	DELETE r;
	CREATE type::record('r', $k) SET k = $k;
	RETURN true
};

-- Each row rewrites `r` and then finds its own key there, so every row is
-- admitted. Reading `r` once for all of them would find only the last key.
SELECT VALUE k FROM t WHERE fn::reset(k) AND k IN (SELECT VALUE k FROM r) ORDER BY k;
SELECT VALUE id FROM r ORDER BY id;
