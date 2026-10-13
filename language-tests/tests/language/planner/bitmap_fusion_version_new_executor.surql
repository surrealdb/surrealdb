/**
[env]
backend = ["mem", "rocksdb", "surrealkv"]
versioned = true
planner-strategy = ["all-ro"]
timeout = 15000

[test]
reason = "VERSION queries never use bitmap fusion plans: doc-ID mappings and index entries are not time-travel-aware, so a fused bitmap (or bitmap count) would reflect current state instead of the requested version"
issue = 547

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ id: t:2 }]"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }]"

[[test.results]]
value = "[{ id: t:2 }, { id: t:4 }, { id: t:5 }]"
*/

DEFINE FIELD a ON t TYPE string;
DEFINE FIELD b ON t TYPE bool;
DEFINE INDEX idx_a ON t FIELDS a;
DEFINE INDEX idx_b ON t FIELDS b;

-- First batch: two matching records (t:1, t:2).
{
    CREATE t:1 SET a = 'x', b = true;
    CREATE t:2 SET a = 'x', b = true;
    CREATE t:3 SET a = 'x', b = false;
    RETURN "OK";
};

SLEEP 50ms;
LET $ts = time::now();
SLEEP 50ms;

-- Mutate after the timestamp: now three matching records (t:2, t:4, t:5).
{
    CREATE t:4 SET a = 'x', b = true;
    CREATE t:5 SET a = 'x', b = true;
    DELETE t:1;
    RETURN "OK";
};

-- VERSION gates bitmap fusion off (both the SELECT plan and the count):
-- doc-ID mappings and index entries reflect current state only. The
-- versioned queries below fall back to the streaming index plans and
-- return exactly what they returned before bitmap fusion existed.
--
-- NOTE: the versioned *indexed* results themselves exhibit a pre-existing
-- engine limitation (unchanged by this feature): index entries are not
-- time-travel-aware, so rows deleted after the timestamp (t:1) are
-- missing from index-driven versioned plans. The NOINDEX baseline shows
-- the true historical answer.
SELECT count() FROM t WHERE a = 'x' AND b = true GROUP ALL VERSION $ts;
SELECT count() FROM t WITH NOINDEX WHERE a = 'x' AND b = true GROUP ALL VERSION $ts;

-- The unversioned count sees current state through the bitmap plan.
SELECT count() FROM t WHERE a = 'x' AND b = true GROUP ALL;

-- Same for row selection.
SELECT id FROM t WHERE a = 'x' AND b = true VERSION $ts;
SELECT id FROM t WITH NOINDEX WHERE a = 'x' AND b = true VERSION $ts;
SELECT id FROM t WHERE a = 'x' AND b = true;
