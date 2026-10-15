/**
[env]
timeout = 30000
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A write whose every event is dormant still materialises the data clause as `$input` before the
events run: compound assignments over a field the record does not hold, over a nested path and
over each kind of value answer, and fail, as they do for a table that holds no event. Every
form of write leaves the same records on both tables, and an event that never fires writes
nothing.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ big: 5, d: d'2020-01-01T00:00:00Z', id: ev:1, n: 1, tags: ['a'] }]"

[[test.results]]
value = "[{ big: 5, d: d'2020-01-01T00:00:00Z', id: plain:1, n: 1, tags: ['a'] }]"

[[test.results]]
value = "[{ big: 3, d: d'2020-01-02T00:00:00Z', id: ev:1, n: 2, tags: ['a', 'b'] }]"

[[test.results]]
value = "[{ big: 3, d: d'2020-01-02T00:00:00Z', id: plain:1, n: 2, tags: ['a', 'b'] }]"

[[test.results]]
value = "[{ big: 3, d: d'2020-01-01T00:00:00Z', id: ev:1, missing: -1, n: 2, nested: { deep: { count: 1 } }, tags: ['b'] }]"

[[test.results]]
value = "[{ big: 3, d: d'2020-01-01T00:00:00Z', id: plain:1, missing: -1, n: 2, nested: { deep: { count: 1 } }, tags: ['b'] }]"

[[test.results]]
error = "Cannot perform addition with '3' and '9223372036854775807'"

[[test.results]]
error = "Cannot perform addition with '3' and '9223372036854775807'"

[[test.results]]
value = """[{ big: 3, d: d'2020-01-01T00:00:00Z', id: ev:1, missing: -1, n: 2, nested: { deep: { count: 1 }, list: { "2": 1 } }, tags: ['z'] }]"""

[[test.results]]
value = """[{ big: 3, d: d'2020-01-01T00:00:00Z', id: plain:1, missing: -1, n: 2, nested: { deep: { count: 1 }, list: { "2": 1 } }, tags: ['z'] }]"""

[[test.results]]
value = """[{ big: 3, d: d'2020-01-01T00:00:00Z', id: ev:1, missing: -1, n: 3, nested: { deep: { count: 1 }, list: { "2": 1 } }, tags: ['z'] }]"""

[[test.results]]
value = "[{ big: -1, id: ev:2, n: 1 }]"

[[test.results]]
value = "[{ big: -1, id: plain:2, n: 1 }]"

[[test.results]]
value = "[{ id: ev:3, n: -1 }]"

[[test.results]]
value = "[{ id: plain:3, n: -1 }]"

[[test.results]]
value = """[{ big: 3, d: d'2020-01-01T00:00:00Z', id: ev:1, missing: -1, n: 13, nested: { deep: { count: 1 }, list: { "2": 1 } }, tags: ['z'] }, { big: -1, id: ev:2, n: 11 }, { id: ev:3, n: 9 }]"""

[[test.results]]
value = """[{ big: 3, d: d'2020-01-01T00:00:00Z', id: plain:1, missing: -1, n: 12, nested: { deep: { count: 1 }, list: { "2": 1 } }, tags: ['z'] }, { big: -1, id: plain:2, n: 11 }, { id: plain:3, n: 9 }]"""

[[test.results]]
value = "[{ id: ev:4, n: 1 }]"

[[test.results]]
value = "[{ id: ev:4, n: 2 }]"

[[test.results]]
value = "[{ id: plain:4, n: 1 }]"

[[test.results]]
value = "[{ id: plain:4, n: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ big: -1, id: ev:2, n: 11 }, { id: ev:3, n: 9 }, { id: ev:4, n: 2 }]"

[[test.results]]
value = "[{ big: -1, id: plain:2, n: 11 }, { id: plain:3, n: 9 }, { id: plain:4, n: 2 }]"

[[test.results]]
value = "[{ count: 0 }]"
*/

DEFINE TABLE ev SCHEMALESS;
DEFINE TABLE plain SCHEMALESS;
DEFINE TABLE audit SCHEMALESS;
-- Every event of `ev` is dormant for every action the statements below run.
DEFINE EVENT d1 ON ev WHEN false THEN (CREATE audit SET tag = 'd1', act = $event);
DEFINE EVENT d2 ON ev WHEN NONE THEN (CREATE audit SET tag = 'd2', act = $event);
DEFINE EVENT d3 ON ev WHEN $event = "NEVER" THEN (CREATE audit SET tag = 'd3', act = $event);
CREATE ev:1 SET big = 5, n = 1, tags = ['a'], d = d'2020-01-01T00:00:00Z';
CREATE plain:1 SET big = 5, n = 1, tags = ['a'], d = d'2020-01-01T00:00:00Z';
-- Compound assignments are materialised over `NONE` as `$input` for a record whose table holds
-- events, whether or not an event fires.
UPDATE ev:1 SET n += 1, big -= 2, tags += 'b', d += 1d;
UPDATE plain:1 SET n += 1, big -= 2, tags += 'b', d += 1d;
UPDATE ev:1 SET tags -= 'a', d -= 1d, missing -= 1, nested.deep.count += 1;
UPDATE plain:1 SET tags -= 'a', d -= 1d, missing -= 1, nested.deep.count += 1;
UPDATE ev:1 SET big += 9223372036854775807;
UPDATE plain:1 SET big += 9223372036854775807;
UPDATE ev:1 SET tags[0] = 'z', nested.list[2] = 1;
UPDATE plain:1 SET tags[0] = 'z', nested.list[2] = 1;
UPSERT ev:1 SET n += 1;
UPSERT ev:2 SET n += 1, big -= 1;
UPSERT plain:2 SET n += 1, big -= 1;
CREATE ev:3 SET n -= 1;
CREATE plain:3 SET n -= 1;
UPDATE ev SET n += 10;
UPDATE plain SET n += 10;
INSERT INTO ev { id: ev:4, n: 1 } ON DUPLICATE KEY UPDATE n += 1;
INSERT INTO ev { id: ev:4, n: 1 } ON DUPLICATE KEY UPDATE n += 1;
INSERT INTO plain { id: plain:4, n: 1 } ON DUPLICATE KEY UPDATE n += 1;
INSERT INTO plain { id: plain:4, n: 1 } ON DUPLICATE KEY UPDATE n += 1;
DELETE ev:1;
DELETE plain:1;
SELECT * FROM ev ORDER BY id;
SELECT * FROM plain ORDER BY id;
SELECT count() FROM audit GROUP ALL;
