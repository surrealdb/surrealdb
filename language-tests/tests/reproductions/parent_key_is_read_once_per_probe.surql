/**
[env]
namespace = true
database = true

[test]
reason = """
A correlation `k = $parent.coin` over a probe of bare record ids becomes a
join key, evaluated once per probe row. That is what the legacy engine does
too: it reads the parent's record once for the subquery, so with `r` holding
both `k = false` and `k = true` every outer row matches, and the count is the
whole table.

The streaming scalar form does not — it dereferences `$parent` again for each
right row, drawing `coin` afresh each time, so an outer row matches only if
one of the draws lands on its own `k`. Keeping the join is therefore what
holds the two engines together here; withdrawing it for a non-object probe
would return a random count near three quarters instead.

Deterministic while the join stands: one draw per probe always equals one of
the two `k` values.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "40"
*/

DEFINE TABLE o SCHEMALESS;
DEFINE TABLE r SCHEMALESS;
FOR $i IN 0..40 { CREATE type::record('o:' + <string> $i) RETURN NONE };
CREATE r:f SET k = false, v = 1 RETURN NONE;
CREATE r:t SET k = true,  v = 1 RETURN NONE;
DEFINE FIELD coin ON o COMPUTED rand::bool();

SELECT VALUE count() FROM ONLY (
  SELECT * FROM (SELECT VALUE id FROM o) WHERE 1 IN (SELECT VALUE v FROM r WHERE k = $parent.coin)
) GROUP ALL;
