/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
An index column is stored as an idiom applied to the record whole, and the
shapes an idiom may take reach the index unchanged: a nested path, a bracket
index, an element filter, and a method call. Every shape must produce the same
value whichever caller evaluates it, so an indexed lookup answers what an
unindexed scan answers.

Each pair of tables below holds identical rows and differs only in when its
index was defined: before the rows, so the write path filled it, or after them,
so the build's scan did.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: a:1, meta: { tier: 1 }, scores: [5, 1], tags: ['x'] }]"

[[test.results]]
value = "[{ id: a:2, meta: { tier: 2 }, scores: [9, 4], tags: ['y'] }]"

[[test.results]]
value = "[{ id: z:1, meta: { tier: 1 }, scores: [5, 1], tags: ['x'] }]"

[[test.results]]
value = "[{ id: z:2, meta: { tier: 2 }, scores: [9, 4], tags: ['y'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[a:2]"

[[test.results]]
value = "[z:2]"

[[test.results]]
value = "[a:2]"

[[test.results]]
value = "[z:2]"

[[test.results]]
value = "[a:1]"

[[test.results]]
value = "[z:1]"

[[test.results]]
value = "[[9]]"

[[test.results]]
value = "[[9]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[a:2]"
*/

-- `a` is indexed before its rows exist, so the write path fills every entry.
DEFINE INDEX a_tier ON a FIELDS meta.tier;
DEFINE INDEX a_top ON a FIELDS scores[0];
DEFINE INDEX a_high ON a FIELDS scores[WHERE $this > 4];

CREATE a:1 SET meta = { tier: 1 }, scores = [5, 1], tags = ["x"];
CREATE a:2 SET meta = { tier: 2 }, scores = [9, 4], tags = ["y"];

-- `z` holds the same rows and is indexed after them, so the build's scan fills
-- every entry.
CREATE z:1 SET meta = { tier: 1 }, scores = [5, 1], tags = ["x"];
CREATE z:2 SET meta = { tier: 2 }, scores = [9, 4], tags = ["y"];

DEFINE INDEX z_tier ON z FIELDS meta.tier;
DEFINE INDEX z_top ON z FIELDS scores[0];
DEFINE INDEX z_high ON z FIELDS scores[WHERE $this > 4];

SELECT VALUE id FROM a WHERE meta.tier = 2;
SELECT VALUE id FROM z WHERE meta.tier = 2;

SELECT VALUE id FROM a WHERE scores[0] = 9;
SELECT VALUE id FROM z WHERE scores[0] = 9;

SELECT VALUE id FROM a WHERE scores[0] = 5;
SELECT VALUE id FROM z WHERE scores[0] = 5;

SELECT VALUE scores[WHERE $this > 4] FROM a WHERE meta.tier = 2;
SELECT VALUE scores[WHERE $this > 4] FROM z WHERE meta.tier = 2;

-- A deleted record's post-image is nullish, which is the absence of an entry
-- rather than a vector of NONEs: every entry the record held is removed, and
-- no entry keyed on NONE is written in its place.
DELETE a:1 RETURN NONE;
SELECT VALUE id FROM a WHERE meta.tier = 1;
SELECT VALUE id FROM a WHERE meta.tier = NONE;
SELECT VALUE id FROM a WHERE meta.tier = 2;
