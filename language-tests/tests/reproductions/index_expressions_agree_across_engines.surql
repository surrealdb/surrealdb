/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
An index's columns and a COUNT index's WHERE guard are one set of expressions
evaluated by two callers: the write path's index stage
and the index build's replay scan. A record entered into an index by a write
and one entered by a build must be judged by the same expressions, or the index
holds keys the other caller would not have written and a query that uses the
index answers differently from one that does not.

Each table below is populated identically and differs only in when its index
was defined: before the writes, so every entry comes from the write path, or
after them, so every entry comes from the build's scan.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: w:a, status: 'active', tier: 1 }]"

[[test.results]]
value = "[{ id: w:b, status: 'inactive', tier: 2 }]"

[[test.results]]
value = "[{ id: w:c, status: 'active', tier: 3 }]"

[[test.results]]
value = "[{ id: b:a, status: 'active', tier: 1 }]"

[[test.results]]
value = "[{ id: b:b, status: 'inactive', tier: 2 }]"

[[test.results]]
value = "[{ id: b:c, status: 'active', tier: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ id: m:1, status: 'active' }]"

[[test.results]]
value = "[{ id: m:2, status: 'inactive' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: m:3, status: 'active' }]"

[[test.results]]
value = "[{ id: m:2, status: 'active' }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: p:1, tier: 1, tags: ['x', 'y'] }]"

[[test.results]]
value = "[{ id: p:2, tier: 2, tags: ['y', 'z'] }]"

[[test.results]]
value = "[{ id: q:1, tier: 1, tags: ['x', 'y'] }]"

[[test.results]]
value = "[{ id: q:2, tier: 2, tags: ['y', 'z'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p:2]"

[[test.results]]
value = "[q:2]"

[[test.results]]
value = "[p:1]"

[[test.results]]
value = "[q:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: u:1, n: 1 }]"

[[test.results]]
value = "[{ id: u:2, n: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ id: v:1, n: 1 }]"

[[test.results]]
value = "[{ id: v:2, n: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 2 }]"
*/

-- --------------------------------------------------------------------------
-- A COUNT index's guard: the write path maintains `w`, the build scan `b`.
-- --------------------------------------------------------------------------

DEFINE INDEX w_active ON w COUNT WHERE status = "active" AND tier < 5;

CREATE w:a SET status = "active", tier = 1;
CREATE w:b SET status = "inactive", tier = 2;
CREATE w:c SET status = "active", tier = 3;

CREATE b:a SET status = "active", tier = 1;
CREATE b:b SET status = "inactive", tier = 2;
CREATE b:c SET status = "active", tier = 3;

DEFINE INDEX b_active ON b COUNT WHERE status = "active" AND tier < 5;

SELECT count() FROM w WHERE status = "active" AND tier < 5 GROUP ALL;
SELECT count() FROM b WHERE status = "active" AND tier < 5 GROUP ALL;

-- --------------------------------------------------------------------------
-- One table whose count is part scan and part write, plus a delete: the
-- emptied post-image must leave the count.
-- --------------------------------------------------------------------------

CREATE m:1 SET status = "active";
CREATE m:2 SET status = "inactive";

DEFINE INDEX m_active ON m COUNT WHERE status = "active";

CREATE m:3 SET status = "active";
UPDATE m:2 SET status = "active";

SELECT count() FROM m WHERE status = "active" GROUP ALL;

DELETE m:1, m:3 RETURN NONE;

SELECT count() FROM m WHERE status = "active" GROUP ALL;

-- --------------------------------------------------------------------------
-- An index's columns: `p` is indexed before its rows, `q` after them. Both
-- answer the same lookups, and both answer what an unindexed scan answers.
-- --------------------------------------------------------------------------

DEFINE INDEX p_tier ON p FIELDS tier;

CREATE p:1 SET tier = 1, tags = ["x", "y"];
CREATE p:2 SET tier = 2, tags = ["y", "z"];

CREATE q:1 SET tier = 1, tags = ["x", "y"];
CREATE q:2 SET tier = 2, tags = ["y", "z"];

DEFINE INDEX q_tier ON q FIELDS tier;

SELECT VALUE id FROM p WHERE tier = 2;
SELECT VALUE id FROM q WHERE tier = 2;

SELECT VALUE id FROM p WHERE tier = 1;
SELECT VALUE id FROM q WHERE tier = 1;

-- --------------------------------------------------------------------------
-- An unguarded COUNT index counts records rather than values, so an image's
-- absence is what keeps it out of the count: the emptied post-image of a
-- delete must leave the count, not add to it.
-- --------------------------------------------------------------------------

DEFINE INDEX u_all ON u COUNT;

CREATE u:1 SET n = 1;
CREATE u:2 SET n = 2;

SELECT count() FROM u GROUP ALL;

DELETE u:1 RETURN NONE;

SELECT count() FROM u GROUP ALL;

CREATE v:1 SET n = 1;
CREATE v:2 SET n = 2;

DEFINE INDEX v_all ON v COUNT;

SELECT count() FROM v GROUP ALL;
