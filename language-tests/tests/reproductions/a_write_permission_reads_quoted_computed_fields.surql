/**
[env]
imports = ["reproductions/a_write_permission_reads_quoted_computed_fields_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "A write `PERMISSIONS` predicate that reads a COMPUTED field whose name has to be quoted (`is-locked`, `verrouillé`) sees the value that field computes to. A read of such a field records the unescaped identifier, so the write gate and the read paths have to know the field by that name: keyed by its escaped form, the field is neither derived for nor ordered before the fields that read it, the predicate reads NONE, and a record user's write to a locked record is admitted."

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ n: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ n: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ n: 1 }]"

[[test.results]]
value = """[{ can_edit: false, id: t:locked, "is-locked": true, status: 'locked' }, { can_edit: true, id: t:open, "is-locked": false, n: 1, status: 'open' }, { id: u:locked, status: 'locked', "verrouillé": true }, { id: u:open, n: 1, status: 'open', "verrouillé": false }, { "can-edit": false, id: v:locked, "is-locked": true, status: 'locked' }, { "can-edit": true, id: v:open, "is-locked": false, n: 1, status: 'open' }]"""

[[test.results]]
value = "[{ can_edit: false, id: t:locked }, { can_edit: true, id: t:open }]"

[[test.results]]
value = "[t:locked]"

*/

-- A predicate field that reads a quoted field: every write to the locked record
-- is rejected, and a rejected record is filtered out of the result.
UPDATE t:locked SET n = 1;
DELETE t:locked RETURN BEFORE;
CREATE t:new SET status = 'locked';
UPDATE t:open SET n = 1 RETURN n;

-- A predicate that reads a quoted field directly.
UPDATE u:locked SET n = 1;
DELETE u:locked RETURN BEFORE;
CREATE u:new SET status = 'locked';
UPDATE u:open SET n = 1 RETURN n;

-- The gate derives every computed field.
UPDATE v:locked SET n = 1;
DELETE v:locked RETURN BEFORE;
CREATE v:new SET status = 'locked';
UPDATE v:open SET n = 1 RETURN n;

-- Nothing was written to a locked record, and the read paths derive the same
-- values the gate judged, under the field's own name.
SELECT * FROM t, u, v ORDER BY id;
SELECT id, can_edit FROM t ORDER BY id;
-- A WHERE reading the quoted field is judged on the derived value, not on the
-- stored record, which does not hold it.
SELECT VALUE id FROM t WHERE `is-locked` = true;
