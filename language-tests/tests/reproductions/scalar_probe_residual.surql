/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A join residual reads the build row through the alias whatever the probe row is: a bare value from a scalar source has no field to hold the alias, and the residual is evaluated against an object carrying it rather than against the bare value"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: r:1, k: 5, n: 2 }]"

[[test.results]]
value = "[0]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ z: 1 }]"
*/
DEFINE TABLE r SCHEMALESS;
CREATE r:1 SET n = 2, k = 5;

-- `n > $parent.z` is the join's residual, and it reads `r`'s row through the
-- alias. Over a bare-value source there is no field to hold the alias; the
-- row is emitted as it is, and `$parent.z` reads `NONE` either way.
SELECT * FROM [0] WHERE 5 IN (SELECT VALUE k FROM r WHERE n > $parent.z);
SELECT * FROM [{ z: 9 }] WHERE 5 IN (SELECT VALUE k FROM r WHERE n > $parent.z);
SELECT * FROM [{ z: 1 }] WHERE 5 IN (SELECT VALUE k FROM r WHERE n > $parent.z);
