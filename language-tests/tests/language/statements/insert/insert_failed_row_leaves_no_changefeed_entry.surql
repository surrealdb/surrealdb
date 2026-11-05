/**
[env]
namespace = true
database = true

[test]
requires = ["changefeed"]
reason = """
A failed INSERT row is rolled back to the save point it opened, and that
rollback has to reach the transaction's buffered changefeed entries as well as
storage: otherwise the feed publishes a write that never committed. Three rows
fail after they have buffered a change — a create attempt whose RETURN
projection throws after the record was created, an `ON DUPLICATE KEY UPDATE`
update whose RETURN projection throws after the update was stored, and an
UPDATE event that writes to a CHANGEFEED table before throwing. Each error is
caught by a `DEFINE API` handler, so the transaction commits. In each case the
feed must list only the writes that survive. Versionstamps are not deterministic,
so the feed is matched on the `update` entries it carries.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "500"

[[test.results]]
value = "[ig:1, ig:3]"

[[test.results]]
match = '''$result.map(|$e| $e.changes).flatten().filter(|$c| $c.update != NONE).map(|$c| $c.update) == [{ id: ig:1 }, { id: ig:3 }]'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "500"

[[test.results]]
value = "[{ id: cf:1, v: 1 }]"

[[test.results]]
match = '''$result.map(|$e| $e.changes).flatten().filter(|$c| $c.update != NONE).map(|$c| $c.update) == [{ id: cf:1, v: 1 }]'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "500"

[[test.results]]
value = "[]"

[[test.results]]
match = '''$result.map(|$e| $e.changes).flatten().filter(|$c| $c.update != NONE).map(|$c| $c.update) == []'''
*/

-- A create attempt whose projection throws after its record was created
DEFINE TABLE ig CHANGEFEED 1h SCHEMALESS;
INSERT INTO ig [{ id: 1 }, { id: 3 }] RETURN NONE;
DEFINE API '/ig' FOR post THEN (INSERT INTO ig { id: 2 } ON DUPLICATE KEY UPDATE v = 1 RETURN VALUE (THROW 'refused'));
api::invoke('/ig', { method: 'post' }).status;
SELECT VALUE id FROM ig ORDER BY id;
SHOW CHANGES FOR TABLE ig SINCE 0;

-- An ON DUPLICATE KEY UPDATE update whose projection throws after it was stored
DEFINE TABLE cf CHANGEFEED 1h SCHEMALESS;
INSERT INTO cf { id: 1, v: 1 } RETURN NONE;
DEFINE API '/cf' FOR post THEN (INSERT INTO cf { id: 1, v: 1 } ON DUPLICATE KEY UPDATE v = 2 RETURN VALUE (THROW 'refused'));
api::invoke('/cf', { method: 'post' }).status;
SELECT id, v FROM cf;
SHOW CHANGES FOR TABLE cf SINCE 0;

-- An UPDATE event that writes to a CHANGEFEED table, then throws
DEFINE TABLE audit CHANGEFEED 1h SCHEMALESS;
DEFINE TABLE ev SCHEMALESS;
DEFINE EVENT log_then_refuse ON ev WHEN $event = 'UPDATE' THEN { CREATE audit:1 SET at = 'update'; THROW 'refused' };
INSERT INTO ev { id: 1, v: 1 } RETURN NONE;
DEFINE API '/ev' FOR post THEN (INSERT INTO ev { id: 1, v: 1 } ON DUPLICATE KEY UPDATE v = 2);
api::invoke('/ev', { method: 'post' }).status;
SELECT * FROM audit;
SHOW CHANGES FOR TABLE audit SINCE 0;
