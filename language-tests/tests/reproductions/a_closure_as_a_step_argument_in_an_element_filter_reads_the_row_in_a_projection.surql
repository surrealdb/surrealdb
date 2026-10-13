/**
[env]
namespace = true
database = true
imports = ["reproductions/a_closure_as_a_step_argument_in_an_element_filter_reads_the_row_in_a_projection_import.surql"]

[test]
reason = "A closure that is an index or method argument in an element filter on a graph path, a field path or a literal array, and reads the row, reads the row the projection is evaluated over: in a `SELECT` the statement runs and answers what the argument written without a closure answers. The same argument without a closure reads the record in the clauses of a write (an UPDATE `SET` and `WHERE`, an UPSERT `WHERE`, a DELETE `WHERE`)."

[[test.results]]
value = "[[sl_gmc_r:1], []]"

[[test.results]]
value = "[[sl_gmt_r:1], []]"

[[test.results]]
value = "[[us_gmt_r:1], []]"

[[test.results]]
value = "[uw_gmt:1]"

[[test.results]]
value = "[pw_gmt:1]"

[[test.results]]
value = "[dw_gmt:1]"

[[test.results]]
value = "[[sl_gic_r:1], []]"

[[test.results]]
value = "[[sl_git_r:1], []]"

[[test.results]]
value = "[[us_git_r:1], []]"

[[test.results]]
value = "[uw_git:1]"

[[test.results]]
value = "[pw_git:1]"

[[test.results]]
value = "[dw_git:1]"

[[test.results]]
value = "[[sl_nmc:2], []]"

[[test.results]]
value = "[[sl_nmt:2], []]"

[[test.results]]
value = "[[us_nmt:2], []]"

[[test.results]]
value = "[uw_nmt:1]"

[[test.results]]
value = "[pw_nmt:1]"

[[test.results]]
value = "[dw_nmt:1]"

[[test.results]]
value = "[[sl_nic:2], []]"

[[test.results]]
value = "[[sl_nit:2], []]"

[[test.results]]
value = "[[us_nit:2], []]"

[[test.results]]
value = "[uw_nit:1]"

[[test.results]]
value = "[pw_nit:1]"

[[test.results]]
value = "[dw_nit:1]"

[[test.results]]
value = "[[3], []]"

[[test.results]]
value = "[[3], []]"

[[test.results]]
value = "[[3], []]"

[[test.results]]
value = "[uw_omt:1]"

[[test.results]]
value = "[pw_omt:1]"

[[test.results]]
value = "[dw_omt:1]"

[[test.results]]
value = "[[3], [3]]"

[[test.results]]
value = "[[3], [3]]"

[[test.results]]
value = "[[3], [3]]"

[[test.results]]
value = "[uw_oit:1, uw_oit:2]"

[[test.results]]
value = "[pw_oit:1, pw_oit:2]"

[[test.results]]
value = "[dw_oit:1, dw_oit:2]"

[[test.results]]
value = "[[10], [1, 5, 10]]"

[[test.results]]
value = "[[10], [1, 5, 10]]"

[[test.results]]
value = "[[10], [1, 5, 10]]"

[[test.results]]
value = "[uw_lmt:1, uw_lmt:2]"

[[test.results]]
value = "[pw_lmt:1, pw_lmt:2]"

[[test.results]]
value = "[dw_lmt:1, dw_lmt:2]"

[[test.results]]
value = "[[10], []]"

[[test.results]]
value = "[[10], []]"

[[test.results]]
value = "[[10], []]"

[[test.results]]
value = "[uw_lit:1]"

[[test.results]]
value = "[pw_lit:1]"

[[test.results]]
value = "[dw_lit:1]"
*/

SELECT VALUE ->sl_gmc_r[WHERE w <= [0].map(|$f| lim)[0]] FROM sl_gmc ORDER BY id;
SELECT VALUE ->sl_gmt_r[WHERE w <= [0].concat([lim])[1]] FROM sl_gmt ORDER BY id;
UPDATE us_gmt SET z = ->us_gmt_r[WHERE w <= [0].concat([lim])[1]] RETURN VALUE z;
UPDATE uw_gmt SET q = 1 WHERE ->uw_gmt_r[WHERE w <= [0].concat([lim])[1]] != [] RETURN VALUE id;
UPSERT pw_gmt SET q = 1 WHERE ->pw_gmt_r[WHERE w <= [0].concat([lim])[1]] != [] RETURN VALUE id;
DELETE dw_gmt WHERE ->dw_gmt_r[WHERE w <= [0].concat([lim])[1]] != [] RETURN VALUE $before.id;
SELECT VALUE ->sl_gic_r[WHERE w <= [0].map(|$f| [0, 8, 100][idx])[0]] FROM sl_gic ORDER BY id;
SELECT VALUE ->sl_git_r[WHERE w <= [0, 8, 100][idx]] FROM sl_git ORDER BY id;
UPDATE us_git SET z = ->us_git_r[WHERE w <= [0, 8, 100][idx]] RETURN VALUE z;
UPDATE uw_git SET q = 1 WHERE ->uw_git_r[WHERE w <= [0, 8, 100][idx]] != [] RETURN VALUE id;
UPSERT pw_git SET q = 1 WHERE ->pw_git_r[WHERE w <= [0, 8, 100][idx]] != [] RETURN VALUE id;
DELETE dw_git WHERE ->dw_git_r[WHERE w <= [0, 8, 100][idx]] != [] RETURN VALUE $before.id;
SELECT VALUE ->sl_nmc_r->sl_nmc[WHERE n >= [0].map(|$f| lim)[0]] FROM sl_nmc ORDER BY id;
SELECT VALUE ->sl_nmt_r->sl_nmt[WHERE n >= [0].concat([lim])[1]] FROM sl_nmt ORDER BY id;
UPDATE us_nmt SET z = ->us_nmt_r->us_nmt[WHERE n >= [0].concat([lim])[1]] RETURN VALUE z;
UPDATE uw_nmt SET q = 1 WHERE ->uw_nmt_r->uw_nmt[WHERE n >= [0].concat([lim])[1]] != [] RETURN VALUE id;
UPSERT pw_nmt SET q = 1 WHERE ->pw_nmt_r->pw_nmt[WHERE n >= [0].concat([lim])[1]] != [] RETURN VALUE id;
DELETE dw_nmt WHERE ->dw_nmt_r->dw_nmt[WHERE n >= [0].concat([lim])[1]] != [] RETURN VALUE $before.id;
SELECT VALUE ->sl_nic_r->sl_nic[WHERE n >= [0].map(|$f| [0, 8, 100][idx])[0]] FROM sl_nic ORDER BY id;
SELECT VALUE ->sl_nit_r->sl_nit[WHERE n >= [0, 8, 100][idx]] FROM sl_nit ORDER BY id;
UPDATE us_nit SET z = ->us_nit_r->us_nit[WHERE n >= [0, 8, 100][idx]] RETURN VALUE z;
UPDATE uw_nit SET q = 1 WHERE ->uw_nit_r->uw_nit[WHERE n >= [0, 8, 100][idx]] != [] RETURN VALUE id;
UPSERT pw_nit SET q = 1 WHERE ->pw_nit_r->pw_nit[WHERE n >= [0, 8, 100][idx]] != [] RETURN VALUE id;
DELETE dw_nit WHERE ->dw_nit_r->dw_nit[WHERE n >= [0, 8, 100][idx]] != [] RETURN VALUE $before.id;
SELECT VALUE o[WHERE w <= [0].map(|$f| lim)[0]].w FROM sl_omc ORDER BY id;
SELECT VALUE o[WHERE w <= [0].concat([lim])[1]].w FROM sl_omt ORDER BY id;
UPDATE us_omt SET z = o[WHERE w <= [0].concat([lim])[1]].w RETURN VALUE z;
UPDATE uw_omt SET q = 1 WHERE o[WHERE w <= [0].concat([lim])[1]].w != [] RETURN VALUE id;
UPSERT pw_omt SET q = 1 WHERE o[WHERE w <= [0].concat([lim])[1]].w != [] RETURN VALUE id;
DELETE dw_omt WHERE o[WHERE w <= [0].concat([lim])[1]].w != [] RETURN VALUE $before.id;
SELECT VALUE o[WHERE w <= [0].map(|$f| [0, 8, 100][idx])[0]].w FROM sl_oic ORDER BY id;
SELECT VALUE o[WHERE w <= [0, 8, 100][idx]].w FROM sl_oit ORDER BY id;
UPDATE us_oit SET z = o[WHERE w <= [0, 8, 100][idx]].w RETURN VALUE z;
UPDATE uw_oit SET q = 1 WHERE o[WHERE w <= [0, 8, 100][idx]].w != [] RETURN VALUE id;
UPSERT pw_oit SET q = 1 WHERE o[WHERE w <= [0, 8, 100][idx]].w != [] RETURN VALUE id;
DELETE dw_oit WHERE o[WHERE w <= [0, 8, 100][idx]].w != [] RETURN VALUE $before.id;
SELECT VALUE [1, 5, 10][WHERE $this >= [0].map(|$f| lim)[0]] FROM sl_lmc ORDER BY id;
SELECT VALUE [1, 5, 10][WHERE $this >= [0].concat([lim])[1]] FROM sl_lmt ORDER BY id;
UPDATE us_lmt SET z = [1, 5, 10][WHERE $this >= [0].concat([lim])[1]] RETURN VALUE z;
UPDATE uw_lmt SET q = 1 WHERE [1, 5, 10][WHERE $this >= [0].concat([lim])[1]] != [] RETURN VALUE id;
UPSERT pw_lmt SET q = 1 WHERE [1, 5, 10][WHERE $this >= [0].concat([lim])[1]] != [] RETURN VALUE id;
DELETE dw_lmt WHERE [1, 5, 10][WHERE $this >= [0].concat([lim])[1]] != [] RETURN VALUE $before.id;
SELECT VALUE [1, 5, 10][WHERE $this >= [0].map(|$f| [0, 8, 100][idx])[0]] FROM sl_lic ORDER BY id;
SELECT VALUE [1, 5, 10][WHERE $this >= [0, 8, 100][idx]] FROM sl_lit ORDER BY id;
UPDATE us_lit SET z = [1, 5, 10][WHERE $this >= [0, 8, 100][idx]] RETURN VALUE z;
UPDATE uw_lit SET q = 1 WHERE [1, 5, 10][WHERE $this >= [0, 8, 100][idx]] != [] RETURN VALUE id;
UPSERT pw_lit SET q = 1 WHERE [1, 5, 10][WHERE $this >= [0, 8, 100][idx]] != [] RETURN VALUE id;
DELETE dw_lit WHERE [1, 5, 10][WHERE $this >= [0, 8, 100][idx]] != [] RETURN VALUE $before.id;
