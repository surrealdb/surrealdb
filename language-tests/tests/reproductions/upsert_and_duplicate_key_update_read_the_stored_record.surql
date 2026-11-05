/**
[env]
namespace = true
database = true

[test]
reason = """
An UPSERT that names a record, and an INSERT whose row collides with one under
ON DUPLICATE KEY UPDATE, evaluate their data clause against the record that is
stored, as an UPDATE does: a field the clause reads is the stored field. A
clause that cannot be evaluated against an empty record (`n + 1` with `n`
absent, a THROW) succeeds on a record that exists, with or without a WHERE,
over a record id written directly, over a generated one, and row by row through
a bulk INSERT. A record that does not exist is still created from the clause
evaluated against the empty record, which raises where the expression does. An
INSERT that collides with nothing evaluates no ON DUPLICATE KEY UPDATE at all,
so a side effect written there happens only for the row that collides.
"""

[[test.results]]
value = "[{ id: q:1, n: 11 }]"

[[test.results]]
value = "[{ a: 12 }]"

[[test.results]]
value = "[{ b: 12 }]"

[[test.results]]
value = "[{ c: 12 }]"

[[test.results]]
value = "[{ d: 12 }]"

[[test.results]]
value = "[{ e: 22 }]"

[[test.results]]
value = "[{ f: 33 }]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot perform addition with 'none' and 'int'"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: q:3 }]"

[[test.results]]
value = "[{ id: q:4 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[['dup']]"

[[test.results]]
value = "['dup']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ amt: 1, id: sl:1 }]"

[[test.results]]
value = "[{ amt: 2 }]"

[[test.results]]
value = "[{ amt: 102 }]"

[[test.results]]
value = "[{ amt: 1 }, { amt: 2 }, { amt: 3 }]"

[[test.results]]
value = "[{ amt: 1020, id: sl:1 }, { amt: 30, id: sl:2 }]"
*/

CREATE q:1 SET n = 11;
UPDATE q:1 SET a = n + 1 RETURN a;
UPSERT q:1 SET b = n + 1 RETURN b;
INSERT INTO q { id: q:1 } ON DUPLICATE KEY UPDATE c = n + 1 RETURN c;
UPSERT q SET d = n + 1 WHERE id = q:1 RETURN d;
UPSERT q:1 SET e = IF n = NONE { THROW 'an empty record' } ELSE { n * 2 } RETURN e;
INSERT INTO q { id: q:1 } ON DUPLICATE KEY UPDATE f = IF n = NONE { THROW 'an empty record' } ELSE { n * 3 } RETURN f;
UPSERT q:1 SET g = n + 1 WHERE n > 100;
UPSERT q:2 SET b = n + 1;
SELECT * FROM q:2;

DEFINE TABLE side SCHEMALESS;
INSERT INTO q { id: q:3 } ON DUPLICATE KEY UPDATE c = n + 1 RETURN id;
INSERT INTO q { id: q:4 } ON DUPLICATE KEY UPDATE c = (CREATE side SET at = 'dup') RETURN id;
SELECT * FROM side;
INSERT INTO q { id: q:4 } ON DUPLICATE KEY UPDATE c = (CREATE side SET at = 'dup') RETURN VALUE c.at;
SELECT VALUE at FROM side;

DEFINE TABLE sl SCHEMALESS;
CREATE sl:1 SET amt = 1;
UPSERT sl:1 SET amt = amt + 1 RETURN amt;
INSERT INTO sl { id: sl:1 } ON DUPLICATE KEY UPDATE amt = amt + 100 RETURN amt;
INSERT INTO sl [{ id: sl:2, amt: 1 }, { id: sl:2, amt: 1 }, { id: sl:2, amt: 1 }] ON DUPLICATE KEY UPDATE amt = amt + 1 RETURN amt;
UPSERT |sl:1..=2| SET amt = amt * 10 RETURN id, amt;
