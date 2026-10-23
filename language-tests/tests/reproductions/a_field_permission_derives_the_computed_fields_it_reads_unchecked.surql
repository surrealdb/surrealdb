/**
[env]
imports = ["reproductions/a_field_permission_derives_the_computed_fields_it_reads_unchecked_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "A COMPUTED field a field's PERMISSIONS FOR select clause reads is derived as the clause itself is evaluated: with reads unchecked, so the field reads what its body would read written in the clause in its place. The caller receives each computed field as the caller would read it. Derived as the caller, a body following a record the caller may not select derives from `NONE`, which satisfies a clause written to deny and fails one written to allow."

[[test.results]]
value = "[{ body: 'public', id: doc:open, note: 'public' }, { body: NONE, id: doc:secret, note: NONE }]"

[[test.results]]
value = "[{ body: NONE, id: doc:secret, note: NONE }]"

[[test.results]]
value = "[{ body: 'public', flag: false, hidden: NONE, id: doc:open, label: 'labelled', meta: meta:open, note: 'public', public: false, summary: 'public', tag: 'tagged' }, { flag: true, hidden: NONE, id: doc:secret, meta: meta:secret, public: false }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[{ ref: { flag: true, hidden: NONE, id: doc:secret, meta: meta:secret, public: false } }]"

[[test.results]]
value = "[[NONE]]"

[[test.results]]
value = "[{ body: NONE, id: doc:secret, note: NONE }]"

[[test.results]]
value = "[{ id: doc:open, label: 'labelled', summary: 'public', tag: 'tagged' }, { id: doc:secret, label: NONE, summary: NONE, tag: NONE }]"

[[test.results]]
value = "[{ hidden: NONE, id: doc:open, public: false }, { hidden: NONE, id: doc:secret, public: false }]"

[[test.results]]
value = "[{ card: { code: 'public', kind: 'memo' }, hidden: NONE, id: deep:open, meta: meta:open }, { card: { kind: 'memo' }, hidden: NONE, id: deep:secret, meta: meta:secret }]"

[[test.results]]
value = "[{ hidden: NONE, id: whole:open, meta: meta:open, remark: 'public' }, { hidden: NONE, id: whole:secret, meta: meta:secret }]"

[[test.results]]
value = "[{ hidden: NONE, id: wide:open, items: [{ code: 'public', n: 1 }], meta: meta:open }, { hidden: NONE, id: wide:secret, items: [{ n: 1 }, { n: 2 }], meta: meta:secret }]"

[[test.results]]
value = "[{ id: ring:1, label: 'ringed', members: 1 }]"

*/

-- Every path a read takes to a guarded field: a scan, a record id, `*`, a
-- predicate reading the field, a link, a `FETCH`, an edge and a write's
-- output.
SELECT id, body, note FROM doc ORDER BY id;
SELECT id, body, note FROM doc:secret;
SELECT * FROM doc ORDER BY id;
SELECT VALUE id FROM doc WHERE body = 'classified';
SELECT VALUE ref.body FROM holder:1;
SELECT ref FROM holder:1 FETCH ref;
SELECT VALUE ->links->doc.body FROM holder:1;
UPDATE doc:secret SET n = 1 RETURN id, body, note;

-- A guard written to allow, and computed fields' own guards.
SELECT id, summary, tag, label FROM doc ORDER BY id;

-- What the caller reads of the computed fields is what the caller may read.
SELECT id, hidden, public FROM doc ORDER BY id;

-- Nested, whole-row and wildcard guards.
SELECT * FROM deep ORDER BY id;
SELECT * FROM whole ORDER BY id;
SELECT * FROM wide ORDER BY id;

-- A computed field counting its own table.
SELECT id, label, members FROM ring;
