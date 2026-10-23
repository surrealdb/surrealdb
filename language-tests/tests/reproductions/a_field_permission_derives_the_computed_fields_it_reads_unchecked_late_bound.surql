/**
[env]
imports = ["reproductions/a_field_permission_derives_the_computed_fields_it_reads_unchecked_import.surql", "reproductions/a_late_bound_statement_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "As a_field_permission_derives_the_computed_fields_it_reads_unchecked, each statement late-bound: planned after a schema statement the plan cannot know the effect of, so it loads its tables' permissions and computed fields when it opens its source."

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

{ LET $late = fn::late('now'); RETURN SELECT id, body, note FROM doc ORDER BY id; };
{ LET $late = fn::late('now'); RETURN SELECT id, body, note FROM doc:secret; };
{ LET $late = fn::late('now'); RETURN SELECT * FROM doc ORDER BY id; };
{ LET $late = fn::late('now'); RETURN SELECT VALUE id FROM doc WHERE body = 'classified'; };
{ LET $late = fn::late('now'); RETURN SELECT VALUE ref.body FROM holder:1; };
{ LET $late = fn::late('now'); RETURN SELECT ref FROM holder:1 FETCH ref; };
{ LET $late = fn::late('now'); RETURN SELECT VALUE ->links->doc.body FROM holder:1; };
{ LET $late = fn::late('now'); RETURN UPDATE doc:secret SET n = 1 RETURN id, body, note; };

{ LET $late = fn::late('now'); RETURN SELECT id, summary, tag, label FROM doc ORDER BY id; };

{ LET $late = fn::late('now'); RETURN SELECT id, hidden, public FROM doc ORDER BY id; };

{ LET $late = fn::late('now'); RETURN SELECT * FROM deep ORDER BY id; };
{ LET $late = fn::late('now'); RETURN SELECT * FROM whole ORDER BY id; };
{ LET $late = fn::late('now'); RETURN SELECT * FROM wide ORDER BY id; };

{ LET $late = fn::late('now'); RETURN SELECT id, label, members FROM ring; };
