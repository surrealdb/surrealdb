/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A record range or a table an expression in FROM evaluates to is read as every record it holds, as a range or table a call yields is; a record id is the record it names. A write whose targets mix such an expression with a range writes every record it names."
issue = 1713

[[test.results]]
value = "[{ id: rr:[1] }, { id: rr:[2] }, { id: rr:[5] }, { id: rr:[6] }]"

[[test.results]]
value = "[rr:[5], rr:[6]]"

[[test.results]]
value = "[rr:[5], rr:[6]]"

[[test.results]]
value = "[rr:[1], rr:[2]]"

[[test.results]]
value = "[rr:[1], rr:[2], rr:[5]]"

[[test.results]]
value = "[rr:[1], rr:[2]]"

[[test.results]]
value = "[rr:[5], rr:[6]]"

[[test.results]]
value = "[rr:[1], rr:[2], rr:[5], rr:[6]]"

[[test.results]]
value = "[rr:[5]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[rr:[1], rr:[2], rr:[5], rr:[6]]"

[[test.results]]
value = "[rr:[6]]"

[[test.results]]
value = "[{ id: ctl:1, go: true }, { id: ctl:2, go: false }]"

[[test.results]]
value = "[{ id: ctl:1, r: [rr:[5], rr:[6]] }, { id: ctl:2, r: [] }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[rr:[1], rr:[2], rr:[5], rr:[6]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[rr:[2]]"

[[test.results]]
error = "The table 'nope' does not exist"

*/

CREATE rr:[1], rr:[2], rr:[5], rr:[6];
-- A range behind a branch the planner cannot fold, and one it can.
SELECT VALUE id FROM (IF rand::int(1, 1) = 1 { rr:[5].. } ELSE { NONE });
SELECT VALUE id FROM (IF true { rr:[5].. } ELSE { NONE });
-- Exclusive, inclusive and open bounds.
SELECT VALUE id FROM (IF rand::int(1, 1) = 1 { rr:[1]..[5] } ELSE { NONE });
SELECT VALUE id FROM (IF rand::int(1, 1) = 1 { rr:[1]..=[5] } ELSE { NONE });
SELECT VALUE id FROM (IF rand::int(1, 1) = 1 { rr:..[5] } ELSE { NONE });
-- An expression other than a branch.
SELECT VALUE id FROM { a: rr:[5].. }.a;
-- A table is read whole; a record id is the record it names.
SELECT VALUE id FROM (IF rand::int(1, 1) = 1 { rr } ELSE { NONE });
SELECT VALUE id FROM (IF rand::int(1, 1) = 1 { rr:[5] } ELSE { NONE });
SELECT VALUE id FROM (IF rand::int(1, 1) = 1 { rr:[9] } ELSE { NONE });
-- The range among other sources.
SELECT VALUE id FROM rr:[1]..=[2], (IF rand::int(1, 1) = 1 { rr:[5].. } ELSE { NONE });
-- Paged as a called range is.
SELECT VALUE id FROM (IF rand::int(1, 1) = 1 { rr:[5].. } ELSE { NONE }) START 1 LIMIT 1;
-- Nested, the range is read for each outer row.
INSERT INTO ctl [{ id: ctl:1, go: true }, { id: ctl:2, go: false }];
SELECT id, (SELECT VALUE id FROM (IF $parent.go { rr:[5].. } ELSE { NONE })) AS r FROM ctl ORDER BY id;
-- A write whose targets mix a range with an expression range.
UPDATE rr:[1]..=[2], (IF rand::int(1, 1) = 1 { rr:[5].. } ELSE { NONE }) SET touched = true RETURN NONE;
SELECT VALUE id FROM rr WHERE touched ORDER BY id;
DELETE rr:[1]..=[1], (IF rand::int(1, 1) = 1 { rr:[5].. } ELSE { NONE });
SELECT VALUE id FROM rr ORDER BY id;
-- A range of a table that does not exist is refused, as a called one is.
SELECT * FROM (IF rand::int(1, 1) = 1 { nope:[5].. } ELSE { NONE });
