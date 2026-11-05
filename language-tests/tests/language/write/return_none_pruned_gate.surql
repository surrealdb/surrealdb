/**
[env]
imports = ["./return_none_pruned_gate_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }
timeout = 30000

[test]
reason = """
A select permission the stages can prove inert (`FULL`, `NONE`, or a predicate
over the session alone) is not judged under `RETURN NONE`, which shows no record.
Twin tables, one written with `RETURN NONE` and one with `RETURN AFTER`, run
every form of write against each such clause as a record user: the creating
order, the updating order of an UPSERT and of an INSERT that conflicts, an
edge, and a delete. Both store the same rows, answer the same errors, and the
`RETURN NONE` write answers no row. A re-RELATE onto an edge the caller may not
select is refused whatever endpoints it names, so the second RELATE of each
`NONE` twin is an error.
"""

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

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: pf_b:1, n: 1 }]"

[[test.results]]
value = "[{ id: pf_b:2, n: 2 }]"

[[test.results]]
value = "[{ id: pf_b:1, n: 11 }, { id: pf_b:2, n: 12 }]"

[[test.results]]
value = "[{ id: pf_b:1, n: 12 }]"

[[test.results]]
value = "[{ id: pf_b:1, n: 100 }]"

[[test.results]]
value = "[{ id: pf_b:3, n: 3 }]"

[[test.results]]
value = "[{ id: pf_b:4, n: 4 }]"

[[test.results]]
value = "[{ id: pf_b:4, n: 41 }]"

[[test.results]]
value = "[{ id: pf_b:5, n: 5 }]"

[[test.results]]
value = "[{ id: pf_b_e:1, in: pf_b:1, n: 1, out: pf_b:2 }]"

[[test.results]]
value = "[{ id: pf_b_e:1, in: pf_b:1, n: 2, out: pf_b:2 }]"

[[test.results]]
value = "[{ id: pf_b_e:1, in: pf_b:1, n: 3, out: pf_b:2 }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ n: 100, rid: 1 }, { n: 3, rid: 3 }, { n: 41, rid: 4 }, { n: 5, rid: 5 }]"

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
error = "Cannot change the `in` or `out` endpoint of an existing edge"

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

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot change the `in` or `out` endpoint of an existing edge"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ n: 100, rid: 1 }, { n: 3, rid: 3 }, { n: 41, rid: 4 }, { n: 5, rid: 5 }]"

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

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: pa_b:1, n: 1 }]"

[[test.results]]
value = "[{ id: pa_b:2, n: 2 }]"

[[test.results]]
value = "[{ id: pa_b:1, n: 11 }, { id: pa_b:2, n: 12 }]"

[[test.results]]
value = "[{ id: pa_b:1, n: 12 }]"

[[test.results]]
value = "[{ id: pa_b:1, n: 100 }]"

[[test.results]]
value = "[{ id: pa_b:3, n: 3 }]"

[[test.results]]
value = "[{ id: pa_b:4, n: 4 }]"

[[test.results]]
value = "[{ id: pa_b:4, n: 41 }]"

[[test.results]]
value = "[{ id: pa_b:5, n: 5 }]"

[[test.results]]
value = "[{ id: pa_b_e:1, in: pa_b:1, n: 1, out: pa_b:2 }]"

[[test.results]]
value = "[{ id: pa_b_e:1, in: pa_b:1, n: 2, out: pa_b:2 }]"

[[test.results]]
value = "[{ id: pa_b_e:1, in: pa_b:1, n: 3, out: pa_b:2 }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ n: 100, rid: 1 }, { n: 3, rid: 3 }, { n: 41, rid: 4 }, { n: 5, rid: 5 }]"

[[test.results]]
value = "[]"
*/

CREATE pf_a:1 SET n = 1 RETURN NONE;
CREATE pf_a:2 SET n = 2 RETURN NONE;
UPDATE pf_a SET n = n + 10 RETURN NONE;
UPDATE pf_a:1 SET n = n + 1 RETURN NONE;
UPSERT pf_a:1 SET n = 100 RETURN NONE;
UPSERT pf_a:3 SET n = 3 RETURN NONE;
INSERT INTO pf_a { id: pf_a:4, n: 4 } RETURN NONE;
INSERT INTO pf_a { id: pf_a:4, n: 40 } ON DUPLICATE KEY UPDATE n = 41 RETURN NONE;
INSERT INTO pf_a { id: pf_a:5, n: 5 } ON DUPLICATE KEY UPDATE n = 41 RETURN NONE;
RELATE pf_a:1->pf_a_e:1->pf_a:2 SET n = 1 RETURN NONE;
RELATE pf_a:1->pf_a_e:1->pf_a:2 SET n = 2 RETURN NONE;
UPDATE pf_a_e SET n = n + 1 RETURN NONE;
DELETE pf_a_e:1 RETURN NONE;
DELETE pf_a:2 RETURN NONE;
DELETE pf_a WHERE n > 100 RETURN NONE;
CREATE pf_b:1 SET n = 1 RETURN AFTER;
CREATE pf_b:2 SET n = 2 RETURN AFTER;
UPDATE pf_b SET n = n + 10 RETURN AFTER;
UPDATE pf_b:1 SET n = n + 1 RETURN AFTER;
UPSERT pf_b:1 SET n = 100 RETURN AFTER;
UPSERT pf_b:3 SET n = 3 RETURN AFTER;
INSERT INTO pf_b { id: pf_b:4, n: 4 } RETURN AFTER;
INSERT INTO pf_b { id: pf_b:4, n: 40 } ON DUPLICATE KEY UPDATE n = 41 RETURN AFTER;
INSERT INTO pf_b { id: pf_b:5, n: 5 } ON DUPLICATE KEY UPDATE n = 41 RETURN AFTER;
RELATE pf_b:1->pf_b_e:1->pf_b:2 SET n = 1 RETURN AFTER;
RELATE pf_b:1->pf_b_e:1->pf_b:2 SET n = 2 RETURN AFTER;
UPDATE pf_b_e SET n = n + 1 RETURN AFTER;
DELETE pf_b_e:1 RETURN AFTER;
DELETE pf_b:2 RETURN AFTER;
DELETE pf_b WHERE n > 100 RETURN AFTER;
RETURN (SELECT n, record::id(id) AS rid FROM pf_a_state ORDER BY rid) = (SELECT n, record::id(id) AS rid FROM pf_b_state ORDER BY rid);
RETURN (SELECT n, record::id(id) AS rid FROM pf_a_e_state ORDER BY rid) = (SELECT n, record::id(id) AS rid FROM pf_b_e_state ORDER BY rid);
SELECT n, record::id(id) AS rid FROM pf_a_state ORDER BY rid;
SELECT n, record::id(id) AS rid FROM pf_a_e_state ORDER BY rid;
CREATE pn_a:1 SET n = 1 RETURN NONE;
CREATE pn_a:2 SET n = 2 RETURN NONE;
UPDATE pn_a SET n = n + 10 RETURN NONE;
UPDATE pn_a:1 SET n = n + 1 RETURN NONE;
UPSERT pn_a:1 SET n = 100 RETURN NONE;
UPSERT pn_a:3 SET n = 3 RETURN NONE;
INSERT INTO pn_a { id: pn_a:4, n: 4 } RETURN NONE;
INSERT INTO pn_a { id: pn_a:4, n: 40 } ON DUPLICATE KEY UPDATE n = 41 RETURN NONE;
INSERT INTO pn_a { id: pn_a:5, n: 5 } ON DUPLICATE KEY UPDATE n = 41 RETURN NONE;
RELATE pn_a:1->pn_a_e:1->pn_a:2 SET n = 1 RETURN NONE;
RELATE pn_a:1->pn_a_e:1->pn_a:2 SET n = 2 RETURN NONE;
UPDATE pn_a_e SET n = n + 1 RETURN NONE;
DELETE pn_a_e:1 RETURN NONE;
DELETE pn_a:2 RETURN NONE;
DELETE pn_a WHERE n > 100 RETURN NONE;
CREATE pn_b:1 SET n = 1 RETURN AFTER;
CREATE pn_b:2 SET n = 2 RETURN AFTER;
UPDATE pn_b SET n = n + 10 RETURN AFTER;
UPDATE pn_b:1 SET n = n + 1 RETURN AFTER;
UPSERT pn_b:1 SET n = 100 RETURN AFTER;
UPSERT pn_b:3 SET n = 3 RETURN AFTER;
INSERT INTO pn_b { id: pn_b:4, n: 4 } RETURN AFTER;
INSERT INTO pn_b { id: pn_b:4, n: 40 } ON DUPLICATE KEY UPDATE n = 41 RETURN AFTER;
INSERT INTO pn_b { id: pn_b:5, n: 5 } ON DUPLICATE KEY UPDATE n = 41 RETURN AFTER;
RELATE pn_b:1->pn_b_e:1->pn_b:2 SET n = 1 RETURN AFTER;
RELATE pn_b:1->pn_b_e:1->pn_b:2 SET n = 2 RETURN AFTER;
UPDATE pn_b_e SET n = n + 1 RETURN AFTER;
DELETE pn_b_e:1 RETURN AFTER;
DELETE pn_b:2 RETURN AFTER;
DELETE pn_b WHERE n > 100 RETURN AFTER;
RETURN (SELECT n, record::id(id) AS rid FROM pn_a_state ORDER BY rid) = (SELECT n, record::id(id) AS rid FROM pn_b_state ORDER BY rid);
RETURN (SELECT n, record::id(id) AS rid FROM pn_a_e_state ORDER BY rid) = (SELECT n, record::id(id) AS rid FROM pn_b_e_state ORDER BY rid);
SELECT n, record::id(id) AS rid FROM pn_a_state ORDER BY rid;
SELECT n, record::id(id) AS rid FROM pn_a_e_state ORDER BY rid;
CREATE pa_a:1 SET n = 1 RETURN NONE;
CREATE pa_a:2 SET n = 2 RETURN NONE;
UPDATE pa_a SET n = n + 10 RETURN NONE;
UPDATE pa_a:1 SET n = n + 1 RETURN NONE;
UPSERT pa_a:1 SET n = 100 RETURN NONE;
UPSERT pa_a:3 SET n = 3 RETURN NONE;
INSERT INTO pa_a { id: pa_a:4, n: 4 } RETURN NONE;
INSERT INTO pa_a { id: pa_a:4, n: 40 } ON DUPLICATE KEY UPDATE n = 41 RETURN NONE;
INSERT INTO pa_a { id: pa_a:5, n: 5 } ON DUPLICATE KEY UPDATE n = 41 RETURN NONE;
RELATE pa_a:1->pa_a_e:1->pa_a:2 SET n = 1 RETURN NONE;
RELATE pa_a:1->pa_a_e:1->pa_a:2 SET n = 2 RETURN NONE;
UPDATE pa_a_e SET n = n + 1 RETURN NONE;
DELETE pa_a_e:1 RETURN NONE;
DELETE pa_a:2 RETURN NONE;
DELETE pa_a WHERE n > 100 RETURN NONE;
CREATE pa_b:1 SET n = 1 RETURN AFTER;
CREATE pa_b:2 SET n = 2 RETURN AFTER;
UPDATE pa_b SET n = n + 10 RETURN AFTER;
UPDATE pa_b:1 SET n = n + 1 RETURN AFTER;
UPSERT pa_b:1 SET n = 100 RETURN AFTER;
UPSERT pa_b:3 SET n = 3 RETURN AFTER;
INSERT INTO pa_b { id: pa_b:4, n: 4 } RETURN AFTER;
INSERT INTO pa_b { id: pa_b:4, n: 40 } ON DUPLICATE KEY UPDATE n = 41 RETURN AFTER;
INSERT INTO pa_b { id: pa_b:5, n: 5 } ON DUPLICATE KEY UPDATE n = 41 RETURN AFTER;
RELATE pa_b:1->pa_b_e:1->pa_b:2 SET n = 1 RETURN AFTER;
RELATE pa_b:1->pa_b_e:1->pa_b:2 SET n = 2 RETURN AFTER;
UPDATE pa_b_e SET n = n + 1 RETURN AFTER;
DELETE pa_b_e:1 RETURN AFTER;
DELETE pa_b:2 RETURN AFTER;
DELETE pa_b WHERE n > 100 RETURN AFTER;
RETURN (SELECT n, record::id(id) AS rid FROM pa_a_state ORDER BY rid) = (SELECT n, record::id(id) AS rid FROM pa_b_state ORDER BY rid);
RETURN (SELECT n, record::id(id) AS rid FROM pa_a_e_state ORDER BY rid) = (SELECT n, record::id(id) AS rid FROM pa_b_e_state ORDER BY rid);
SELECT n, record::id(id) AS rid FROM pa_a_state ORDER BY rid;
SELECT n, record::id(id) AS rid FROM pa_a_e_state ORDER BY rid;
