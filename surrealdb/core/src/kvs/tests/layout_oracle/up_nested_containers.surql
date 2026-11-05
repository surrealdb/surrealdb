-- In-place upgrade with nested containers written before @upgrade: objects with declared
-- and FLEXIBLE sub-fields, arrays of objects, sets, and NONE and NULL inside containers.
-- After the upgrade, partial updates of those legacy rows must change only the path written,
-- indexes over nested paths must answer as a scan does, and a migration over a nested path
-- must read the value the legacy row holds.
DEFINE TABLE box SCHEMAFULL;
DEFINE FIELD meta ON box TYPE object;
DEFINE FIELD meta.score ON box TYPE option<int>;
DEFINE FIELD meta.tags ON box TYPE option<array<string>>;
DEFINE FIELD items ON box TYPE array<object>;
DEFINE FIELD items.*.price ON box TYPE option<decimal>;
DEFINE FIELD items.*.qty ON box TYPE int;
DEFINE FIELD seen ON box TYPE option<set<int>>;
DEFINE FIELD note ON box TYPE option<string>;
DEFINE FIELD extra ON box TYPE option<object> FLEXIBLE;
DEFINE FIELD stamp ON box TYPE option<string> DEFAULT 'none';
DEFINE FIELD tag_count ON box VALUE array::len(meta.tags ?? []) TYPE option<int>;
DEFINE INDEX by_tag ON box FIELDS meta.tags;
DEFINE INDEX by_seen ON box FIELDS seen;
DEFINE INDEX by_score ON box FIELDS meta.score;
CREATE box:1 SET meta = { score: 10, tags: ['a', 'b'] }, items = [{ price: 1.25dec, qty: 2 }, { qty: 1 }], seen = <set>[3, 1, 3], extra = { k: { deep: [1, 'two', NULL] } };
CREATE box:2 SET meta = { tags: [] }, items = [], seen = <set>[], extra = { a: NULL, b: NONE };
CREATE box:3 SET meta = { score: -5 }, items = [{ qty: 4, price: 0.5dec }, { qty: 0 }], note = NONE;
CREATE box:4 SET meta = { score: 7, tags: ['x', 'x'] }, items = [{ qty: 9 }], seen = <set>[10, 2], note = 'héllo wörld', extra = { nested: { a: { b: { c: 'deep' } } } };
CREATE box:5 SET meta = {}, items = [{ qty: 1, price: 2.5dec }], seen = <set>[7];
-- @upgrade
SELECT * FROM box ORDER BY id;
SELECT VALUE id FROM box WHERE meta.tags CONTAINS 'x' ORDER BY id;
SELECT VALUE id FROM box WHERE seen CONTAINS 3 ORDER BY id;
SELECT VALUE id FROM box WHERE meta.score > 0 ORDER BY id;
SELECT VALUE id FROM box WHERE meta.score = -5 ORDER BY id;
SELECT VALUE extra.nested.a.b.c FROM box ORDER BY id;
SELECT VALUE stamp FROM box ORDER BY id;
SELECT VALUE tag_count FROM box ORDER BY id;
UPDATE box:1 SET meta.score += 5 RETURN AFTER;
UPDATE box:1 SET items[1].qty = 3 RETURN AFTER;
UPDATE box:2 SET meta.tags += 'new' RETURN AFTER;
UPDATE box:3 UNSET meta.score RETURN AFTER;
UPDATE box:4 SET extra.nested.a.b.c = 'changed' RETURN AFTER;
UPDATE box:5 SET seen += 4 RETURN AFTER;
SELECT VALUE id FROM box WHERE meta.score = -5 ORDER BY id;
SELECT VALUE id FROM box WHERE meta.tags CONTAINS 'new' ORDER BY id;
SELECT VALUE id FROM box WHERE seen CONTAINS 4 ORDER BY id;
-- @check
REMOVE INDEX by_score ON box;
REMOVE INDEX by_tag ON box;
ALTER FIELD meta.score ON box TYPE option<float> MIGRATE EXISTING ROWS AS score_f USING <float>$value FALLBACK 0.5;
SELECT * FROM box ORDER BY id;
-- @drain
DEFINE INDEX by_score ON box FIELDS meta.score;
DEFINE INDEX by_tag ON box FIELDS meta.tags;
SELECT VALUE meta.score FROM box ORDER BY id;
SELECT * FROM box ORDER BY id;
SELECT VALUE id FROM box WHERE meta.score > 6 ORDER BY id;
(INFO FOR TABLE box).migrations;
-- @check
CREATE box:6 SET meta = { score: 1.5, tags: ['z'] }, items = [{ qty: 5 }], seen = <set>[1];
UPDATE box:4 SET items = [{ qty: 1 }, { qty: 2, price: 3.5dec }] RETURN AFTER;
UPDATE box:4 SET note = NONE RETURN AFTER;
SELECT * FROM box ORDER BY id;
SELECT VALUE id FROM box WHERE meta.score >= 1.5 ORDER BY id;
-- @check
