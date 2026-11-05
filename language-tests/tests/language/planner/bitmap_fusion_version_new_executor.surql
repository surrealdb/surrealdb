/**
[env]
backend = ["mem", "surrealkv"]
versioned = true
timeout = 15000

[test]
reason = "VERSION queries never use an index path, bitmap fusion plans included: doc-ID mappings and index entries are not time-travel-aware, so a fused bitmap (or bitmap count) would reflect current state instead of the requested version. The versioned read scans the table at the stamp, which is what its WITH NOINDEX twin does."
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
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }]"

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

-- VERSION gates every index path off (both the SELECT plan and the count):
-- index entries and doc-ID mappings reflect current state only, so each
-- versioned query is the scan at the stamp and must agree with its
-- WITH NOINDEX twin.
SELECT count() FROM t WHERE a = 'x' AND b = true GROUP ALL VERSION $ts;
SELECT count() FROM t WITH NOINDEX WHERE a = 'x' AND b = true GROUP ALL VERSION $ts;

-- The unversioned count sees current state through the bitmap plan.
SELECT count() FROM t WHERE a = 'x' AND b = true GROUP ALL;

-- Same for row selection.
SELECT id FROM t WHERE a = 'x' AND b = true VERSION $ts;
SELECT id FROM t WITH NOINDEX WHERE a = 'x' AND b = true VERSION $ts;
SELECT id FROM t WHERE a = 'x' AND b = true;
