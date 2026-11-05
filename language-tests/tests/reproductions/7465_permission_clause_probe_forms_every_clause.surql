/**
[env]
imports = ["reproductions/7465_permission_clause_probe_forms_every_clause_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "A permission clause is definer code, so it reads other tables unchecked however it spells the read and whichever clause it is: a record-id dereference, a subquery, `record::exists` and a graph traversal each admit through tables the caller cannot read, in a table select, create, update and delete clause and in a field select, create and update clause alike, and each denies where the record it probes for does not exist."
issue = 7465

[[test.results]]
value = "[{ hidden: NONE, id: deref_ok:seed, secret: 's' }]"

[[test.results]]
value = "[{ id: deref_ok:new }]"

[[test.results]]
value = "[{ id: deref_ok:seed }]"

[[test.results]]
value = "[{ w_no: NONE, w_ok: 1 }]"

[[test.results]]
value = "[{ w_no: NONE, w_ok: 2 }]"

[[test.results]]
value = "[deref_ok:seed]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ hidden: NONE, id: subquery_ok:seed, secret: 's' }]"

[[test.results]]
value = "[{ id: subquery_ok:new }]"

[[test.results]]
value = "[{ id: subquery_ok:seed }]"

[[test.results]]
value = "[{ w_no: NONE, w_ok: 1 }]"

[[test.results]]
value = "[{ w_no: NONE, w_ok: 2 }]"

[[test.results]]
value = "[subquery_ok:seed]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ hidden: NONE, id: exists_ok:seed, secret: 's' }]"

[[test.results]]
value = "[{ id: exists_ok:new }]"

[[test.results]]
value = "[{ id: exists_ok:seed }]"

[[test.results]]
value = "[{ w_no: NONE, w_ok: 1 }]"

[[test.results]]
value = "[{ w_no: NONE, w_ok: 2 }]"

[[test.results]]
value = "[exists_ok:seed]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ hidden: NONE, id: graph_ok:seed, secret: 's' }]"

[[test.results]]
value = "[{ id: graph_ok:new }]"

[[test.results]]
value = "[{ id: graph_ok:seed }]"

[[test.results]]
value = "[{ w_no: NONE, w_ok: 1 }]"

[[test.results]]
value = "[{ w_no: NONE, w_ok: 2 }]"

[[test.results]]
value = "[graph_ok:seed]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/

SELECT id, secret, hidden FROM deref_ok:seed;
CREATE deref_ok:new SET secret = 's' RETURN id;
UPDATE deref_ok:seed SET touched = true RETURN id;
CREATE deref_ok:fw SET w_ok = 1, w_no = 1 RETURN w_ok, w_no;
UPDATE deref_ok:fw SET w_ok = 2, w_no = 2 RETURN w_ok, w_no;
DELETE deref_ok:seed RETURN BEFORE.id;
SELECT id, secret FROM deref_no:seed;
CREATE deref_no:new SET secret = 's' RETURN id;
UPDATE deref_no:seed SET touched = true RETURN id;
DELETE deref_no:seed RETURN BEFORE.id;
SELECT id, secret, hidden FROM subquery_ok:seed;
CREATE subquery_ok:new SET secret = 's' RETURN id;
UPDATE subquery_ok:seed SET touched = true RETURN id;
CREATE subquery_ok:fw SET w_ok = 1, w_no = 1 RETURN w_ok, w_no;
UPDATE subquery_ok:fw SET w_ok = 2, w_no = 2 RETURN w_ok, w_no;
DELETE subquery_ok:seed RETURN BEFORE.id;
SELECT id, secret FROM subquery_no:seed;
CREATE subquery_no:new SET secret = 's' RETURN id;
UPDATE subquery_no:seed SET touched = true RETURN id;
DELETE subquery_no:seed RETURN BEFORE.id;
SELECT id, secret, hidden FROM exists_ok:seed;
CREATE exists_ok:new SET secret = 's' RETURN id;
UPDATE exists_ok:seed SET touched = true RETURN id;
CREATE exists_ok:fw SET w_ok = 1, w_no = 1 RETURN w_ok, w_no;
UPDATE exists_ok:fw SET w_ok = 2, w_no = 2 RETURN w_ok, w_no;
DELETE exists_ok:seed RETURN BEFORE.id;
SELECT id, secret FROM exists_no:seed;
CREATE exists_no:new SET secret = 's' RETURN id;
UPDATE exists_no:seed SET touched = true RETURN id;
DELETE exists_no:seed RETURN BEFORE.id;
SELECT id, secret, hidden FROM graph_ok:seed;
CREATE graph_ok:new SET secret = 's' RETURN id;
UPDATE graph_ok:seed SET touched = true RETURN id;
CREATE graph_ok:fw SET w_ok = 1, w_no = 1 RETURN w_ok, w_no;
UPDATE graph_ok:fw SET w_ok = 2, w_no = 2 RETURN w_ok, w_no;
DELETE graph_ok:seed RETURN BEFORE.id;
SELECT id, secret FROM graph_no:seed;
CREATE graph_no:new SET secret = 's' RETURN id;
UPDATE graph_no:seed SET touched = true RETURN id;
DELETE graph_no:seed RETURN BEFORE.id;
SELECT * FROM lookup;
SELECT * FROM linked;
