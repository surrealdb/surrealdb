/**
[env]
imports = ["reproductions/1718_a_mixed_write_stores_the_record_not_the_callers_view_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:u" }

[test]
reason = "A write whose targets mix a range with an expression writes each record the expression names as it is stored, not as the record user may see it: a field the user may not select survives the write, and an object the expression yields names the record under its id, so neither its fields nor its claim to pass the update permission reach storage."
issue = 1718

[[test.results]]
value = "[{ id: rr:[1], open: true, touched: true }, { id: rr:[5], open: true, touched: true }]"

[[test.results]]
value = "[{ of: rr:[1], secret: 's1' }, { of: rr:[5], secret: 's5' }]"

[[test.results]]
value = "[rr:[1]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[rr:[1], rr:[5]]"

[[test.results]]
value = "[{ of: rr:[1], secret: 's1' }, { of: rr:[1], secret: 's1' }, { of: rr:[5], secret: 's5' }, { of: rr:[5], secret: 's5' }]"

[[test.results]]
value = "[rr:[1], rr:[5]]"

*/

-- `secret` is kept on both records, though the user is shown neither.
UPDATE rr:[1]..=[1], (IF rand::int(1, 1) = 1 { rr:[5] } ELSE { NONE }) SET touched = true RETURN AFTER;
SELECT of, secret FROM seen ORDER BY of;

-- A forged object claiming `open` does not pass the update permission the
-- stored `rr:[6]` fails.
UPDATE rr:[1]..=[1], (IF rand::int(1, 1) = 1 { { id: rr:[6], open: true, secret: 'forged' } } ELSE { NONE }) SET forged = true RETURN VALUE id;
SELECT VALUE id FROM rr:[6];

-- A forged object naming a record the user may update writes the stored
-- record, its `secret` included, not the object's fields.
UPDATE rr:[1]..=[1], (IF rand::int(1, 1) = 1 { { id: rr:[5], secret: 'forged' } } ELSE { NONE }) SET forged = true RETURN VALUE id;
SELECT of, secret FROM seen ORDER BY of;
SELECT VALUE id FROM rr WHERE forged ORDER BY id;
