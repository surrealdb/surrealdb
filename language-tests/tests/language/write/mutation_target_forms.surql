/**
[env]
namespace = true
database = true

[test]
reason = """
The target forms an updating statement accepts, run against UPDATE, UPSERT,
DELETE and CREATE so that each form is covered for more than one statement.
Record-id ranges and mocks are the thin ones: both appear in barely a dozen
files across the suite, and almost always for SELECT.

Most forms resolve to the same set of records whichever statement names them: a
record id, an array of ids, an object carrying an `id`, a param, a subquery, a
graph lookup, and a bare table.

Two do not. A mock names records to invent rather than records to find, so
`CREATE |mk:2|` makes two records, while the same mock as an UPDATE target
generates two fresh ids that match nothing — no rows returned, none created,
and the count afterwards unchanged. A record-id range is the reverse: UPDATE,
UPSERT and DELETE each resolve one, and CREATE rejects it outright.

The edge is given an explicit id and every mock row is projected down to the
field being written, so no generated id reaches a result.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, n: 1 }]"

[[test.results]]
value = "[{ id: t:2, n: 2 }]"

[[test.results]]
value = "[{ id: t:3, n: 3 }]"

[[test.results]]
value = "[{ id: link:e1, in: t:1, out: t:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ u: 'id' }]"

[[test.results]]
value = "[{ u: 'range' }, { u: 'range' }, { u: 'range' }]"

[[test.results]]
value = "[{ u: 'array' }, { u: 'array' }]"

[[test.results]]
value = "[{ u: 'object' }]"

[[test.results]]
value = "[{ u: 'param' }]"

[[test.results]]
value = "[{ u: 'subquery' }]"

[[test.results]]
value = "[{ u: 'graph' }]"

[[test.results]]
value = "[{ u: 'table' }, { u: 'table' }, { u: 'table' }]"

[[test.results]]
value = "[{ u: 'mock' }, { u: 'mock' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ v: 'id' }]"

[[test.results]]
value = "[{ v: 'range' }, { v: 'range' }, { v: 'range' }]"

[[test.results]]
value = "[{ v: 'array' }, { v: 'array' }]"

[[test.results]]
value = "[{ v: 'object' }]"

[[test.results]]
value = "[{ v: 'param' }]"

[[test.results]]
value = "[{ v: 'subquery' }]"

[[test.results]]
value = "[{ v: 'graph' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: dr:1 }]"

[[test.results]]
value = "[{ id: dr:2 }]"

[[test.results]]
value = "[{ id: dr:3 }]"

[[test.results]]
value = "[{ id: dr:4 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: dr:1 }, { id: dr:4 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: dx:1 }]"

[[test.results]]
value = "[{ id: dx:2 }]"

[[test.results]]
value = "[{ id: dx:3 }]"

[[test.results]]
value = "[{ id: dx:4 }]"

[[test.results]]
value = "[{ id: dlink:e2, in: dx:1, out: dx:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: dx:1 }, { id: dx:2 }, { id: dx:3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: dx:1 }, { id: dx:2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: dx:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ k: 'id' }]"

[[test.results]]
value = "[{ k: 'table' }]"

[[test.results]]
value = "[{ k: 'array' }, { k: 'array' }]"

[[test.results]]
value = "[{ k: 'object' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ k: 'param' }]"

[[test.results]]
value = "[{ k: 'mock' }, { k: 'mock' }]"

[[test.results]]
error = "Cannot execute CREATE statement using value: c:6..=8"



















































































































































*/

-- Fixtures. The edge is given an explicit id, and the mock targets project
-- away the generated ids, so every result below is stable.
DEFINE TABLE t SCHEMALESS;
DEFINE TABLE mk SCHEMALESS;
CREATE t:1 SET n = 1;
CREATE t:2 SET n = 2;
CREATE t:3 SET n = 3;
RELATE t:1->link:e1->t:2;
LET $p = t:1;

-- UPDATE reaches each target form.
UPDATE t:1 SET u = 'id' RETURN u;
UPDATE t:1..=3 SET u = 'range' RETURN u;
UPDATE [t:1, t:3] SET u = 'array' RETURN u;
UPDATE { id: t:1 } SET u = 'object' RETURN u;
UPDATE $p SET u = 'param' RETURN u;
UPDATE (SELECT * FROM t WHERE n = 3) SET u = 'subquery' RETURN u;
UPDATE t:1->link->t SET u = 'graph' RETURN u;
UPDATE t SET u = 'table' RETURN u;
CREATE |mk:2| SET u = 'mock' RETURN u;
UPDATE |mk:2| SET u = 'remock' RETURN u;
SELECT count() FROM mk GROUP ALL;

-- UPSERT reaches the same forms. Against a bare table it creates one record
-- rather than matching, which is why the table row is last and projected.
UPSERT t:1 SET v = 'id' RETURN v;
UPSERT t:1..=3 SET v = 'range' RETURN v;
UPSERT [t:1, t:3] SET v = 'array' RETURN v;
UPSERT { id: t:1 } SET v = 'object' RETURN v;
UPSERT $p SET v = 'param' RETURN v;
UPSERT (SELECT * FROM t WHERE n = 3) SET v = 'subquery' RETURN v;
UPSERT t:1->link->t SET v = 'graph' RETURN v;

-- DELETE reaches the same forms, counted by what is left afterwards.
DELETE t:3;
SELECT count() FROM t GROUP ALL;
DELETE [t:2];
SELECT count() FROM t GROUP ALL;
DELETE (SELECT * FROM t WHERE n = 1);
SELECT count() FROM t GROUP ALL;

-- A record-id range as a DELETE target, on its own table so that what the
-- range spans and what it leaves behind are both visible.
DEFINE TABLE dr SCHEMALESS;
CREATE dr:1;
CREATE dr:2;
CREATE dr:3;
CREATE dr:4;
DELETE dr:2..=3;
SELECT id FROM dr;

-- The remaining forms as DELETE targets, one per step so that what each form
-- matched shows up in what survives.
DEFINE TABLE dx SCHEMALESS;
CREATE dx:1;
CREATE dx:2;
CREATE dx:3;
CREATE dx:4;
RELATE dx:1->dlink:e2->dx:2;
LET $r = dx:3;
DELETE { id: dx:4 };
SELECT id FROM dx;
DELETE $r;
SELECT id FROM dx;
DELETE dx:1->dlink->dx;
SELECT id FROM dx;
DELETE dx;
SELECT id FROM dx;

-- CREATE takes the same forms, with two differences: a mock works, because a
-- mock names records to invent, and a record-id range does not.
DEFINE TABLE c SCHEMALESS;
CREATE c:1 SET k = 'id' RETURN k;
CREATE c SET k = 'table' RETURN k;
CREATE [c:2, c:3] SET k = 'array' RETURN k;
CREATE { id: c:4 } SET k = 'object' RETURN k;
LET $q = c:5;
CREATE $q SET k = 'param' RETURN k;
CREATE |c:2| SET k = 'mock' RETURN k;
CREATE c:6..=8 SET k = 'range' RETURN k;
