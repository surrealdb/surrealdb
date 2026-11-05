/**
[env]
namespace = true
database = true
imports = ["reproductions/a_write_clause_element_filter_on_a_graph_or_field_path_reads_arguments_against_the_record_import.surql"]

[test]
reason = "An element filter on a graph path or on a field path, in the clauses of a write, in a field's `VALUE`, in an event's `WHEN` and in a view's projection, whose body reads the record through an index argument or a method argument, reads the record the write judges, as the same filter on a literal array does: the statement runs, and a closure in the filter answers what the same filter written without a closure answers. The positions are an UPDATE `WHERE`, `SET` and `RETURN`, a DELETE `WHERE`, an UPSERT `WHERE`, a field `VALUE`, an event `WHEN` and a view projection; a closure in a write's `WHERE` is declined, so those positions hold the filter written without a closure alone."

[[test.results]]
value = "[[us_gci_r:1], []]"

[[test.results]]
value = "[[ur_gci_r:1], []]"

[[test.results]]
value = "[[fv_gci_r:1], []]"

[[test.results]]
value = "[ev_gci:1, ev_gci:2]"

[[test.results]]
value = "[ev_gci:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[vw_gci_r:1], []]"

[[test.results]]
value = "[[us_gcm_r:1], []]"

[[test.results]]
value = "[[ur_gcm_r:1], []]"

[[test.results]]
value = "[[fv_gcm_r:1], []]"

[[test.results]]
value = "[ev_gcm:1, ev_gcm:2]"

[[test.results]]
value = "[ev_gcm:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[vw_gcm_r:1], []]"

[[test.results]]
value = "[[{ idx: 0, lim: 2, w: 3 }], [{ idx: 0, lim: 2, w: 3 }]]"

[[test.results]]
value = "[[{ idx: 0, lim: 2, w: 3 }], [{ idx: 0, lim: 2, w: 3 }]]"

[[test.results]]
value = "[[{ idx: 0, lim: 2, w: 3 }], [{ idx: 0, lim: 2, w: 3 }]]"

[[test.results]]
value = "[ev_oci:1, ev_oci:2]"

[[test.results]]
value = "[ev_oci:1, ev_oci:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[{ idx: 0, lim: 2, w: 3 }], [{ idx: 0, lim: 2, w: 3 }]]"

[[test.results]]
value = "[[{ idx: 0, lim: 2, w: 3 }], []]"

[[test.results]]
value = "[[{ idx: 0, lim: 2, w: 3 }], []]"

[[test.results]]
value = "[[{ idx: 0, lim: 2, w: 3 }], []]"

[[test.results]]
value = "[ev_ocm:1, ev_ocm:2]"

[[test.results]]
value = "[ev_ocm:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[{ idx: 0, lim: 2, w: 3 }], []]"

[[test.results]]
value = "[uw_gfi:1]"

[[test.results]]
value = "[[us_gfi_r:1], []]"

[[test.results]]
value = "[[ur_gfi_r:1], []]"

[[test.results]]
value = "[dw_gfi:1]"

[[test.results]]
value = "[pw_gfi:1]"

[[test.results]]
value = "[[fv_gfi_r:1], []]"

[[test.results]]
value = "[ev_gfi:1, ev_gfi:2]"

[[test.results]]
value = "[ev_gfi:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[vw_gfi_r:1], []]"

[[test.results]]
value = "[uw_gfm:1]"

[[test.results]]
value = "[[us_gfm_r:1], []]"

[[test.results]]
value = "[[ur_gfm_r:1], []]"

[[test.results]]
value = "[dw_gfm:1]"

[[test.results]]
value = "[pw_gfm:1]"

[[test.results]]
value = "[[fv_gfm_r:1], []]"

[[test.results]]
value = "[ev_gfm:1, ev_gfm:2]"

[[test.results]]
value = "[ev_gfm:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[vw_gfm_r:1], []]"

[[test.results]]
value = "[uw_ofi:1, uw_ofi:2]"

[[test.results]]
value = "[[{ idx: 0, lim: 2, w: 3 }], [{ idx: 0, lim: 2, w: 3 }]]"

[[test.results]]
value = "[[{ idx: 0, lim: 2, w: 3 }], [{ idx: 0, lim: 2, w: 3 }]]"

[[test.results]]
value = "[dw_ofi:1, dw_ofi:2]"

[[test.results]]
value = "[pw_ofi:1, pw_ofi:2]"

[[test.results]]
value = "[[{ idx: 0, lim: 2, w: 3 }], [{ idx: 0, lim: 2, w: 3 }]]"

[[test.results]]
value = "[ev_ofi:1, ev_ofi:2]"

[[test.results]]
value = "[ev_ofi:1, ev_ofi:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[{ idx: 0, lim: 2, w: 3 }], [{ idx: 0, lim: 2, w: 3 }]]"

[[test.results]]
value = "[uw_ofm:1]"

[[test.results]]
value = "[[{ idx: 0, lim: 2, w: 3 }], []]"

[[test.results]]
value = "[[{ idx: 0, lim: 2, w: 3 }], []]"

[[test.results]]
value = "[dw_ofm:1]"

[[test.results]]
value = "[pw_ofm:1]"

[[test.results]]
value = "[[{ idx: 0, lim: 2, w: 3 }], []]"

[[test.results]]
value = "[ev_ofm:1, ev_ofm:2]"

[[test.results]]
value = "[ev_ofm:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[{ idx: 0, lim: 2, w: 3 }], []]"
*/

UPDATE us_gci SET z = ->us_gci_r[WHERE array::any([w], |$f| $f <= [0, 8, 100][idx])] RETURN VALUE z;
UPDATE ur_gci SET q = 1 RETURN VALUE ->ur_gci_r[WHERE array::any([w], |$f| $f <= [0, 8, 100][idx])];
UPDATE fv_gci SET q = 1 RETURN VALUE z;
UPDATE ev_gci SET q = 1 RETURN VALUE id;
SELECT VALUE src FROM ev_gci_log ORDER BY src;
UPDATE vw_gci SET q = 1 RETURN NONE;
SELECT VALUE ws FROM vw_gci_v ORDER BY id;
UPDATE us_gcm SET z = ->us_gcm_r[WHERE array::any([w], |$f| $f <= [0].concat([lim]).max())] RETURN VALUE z;
UPDATE ur_gcm SET q = 1 RETURN VALUE ->ur_gcm_r[WHERE array::any([w], |$f| $f <= [0].concat([lim]).max())];
UPDATE fv_gcm SET q = 1 RETURN VALUE z;
UPDATE ev_gcm SET q = 1 RETURN VALUE id;
SELECT VALUE src FROM ev_gcm_log ORDER BY src;
UPDATE vw_gcm SET q = 1 RETURN NONE;
SELECT VALUE ws FROM vw_gcm_v ORDER BY id;
UPDATE us_oci SET z = o[WHERE array::any([w], |$f| $f <= [0, 8, 100][idx])] RETURN VALUE z;
UPDATE ur_oci SET q = 1 RETURN VALUE o[WHERE array::any([w], |$f| $f <= [0, 8, 100][idx])];
UPDATE fv_oci SET q = 1 RETURN VALUE z;
UPDATE ev_oci SET q = 1 RETURN VALUE id;
SELECT VALUE src FROM ev_oci_log ORDER BY src;
UPDATE vw_oci SET q = 1 RETURN NONE;
SELECT VALUE ws FROM vw_oci_v ORDER BY id;
UPDATE us_ocm SET z = o[WHERE array::any([w], |$f| $f <= [0].concat([lim]).max())] RETURN VALUE z;
UPDATE ur_ocm SET q = 1 RETURN VALUE o[WHERE array::any([w], |$f| $f <= [0].concat([lim]).max())];
UPDATE fv_ocm SET q = 1 RETURN VALUE z;
UPDATE ev_ocm SET q = 1 RETURN VALUE id;
SELECT VALUE src FROM ev_ocm_log ORDER BY src;
UPDATE vw_ocm SET q = 1 RETURN NONE;
SELECT VALUE ws FROM vw_ocm_v ORDER BY id;
UPDATE uw_gfi SET q = 1 WHERE ->uw_gfi_r[WHERE w <= [0, 8, 100][idx]] != [] RETURN VALUE id;
UPDATE us_gfi SET z = ->us_gfi_r[WHERE w <= [0, 8, 100][idx]] RETURN VALUE z;
UPDATE ur_gfi SET q = 1 RETURN VALUE ->ur_gfi_r[WHERE w <= [0, 8, 100][idx]];
DELETE dw_gfi WHERE ->dw_gfi_r[WHERE w <= [0, 8, 100][idx]] != [] RETURN VALUE $before.id;
UPSERT pw_gfi SET q = 1 WHERE ->pw_gfi_r[WHERE w <= [0, 8, 100][idx]] != [] RETURN VALUE id;
UPDATE fv_gfi SET q = 1 RETURN VALUE z;
UPDATE ev_gfi SET q = 1 RETURN VALUE id;
SELECT VALUE src FROM ev_gfi_log ORDER BY src;
UPDATE vw_gfi SET q = 1 RETURN NONE;
SELECT VALUE ws FROM vw_gfi_v ORDER BY id;
UPDATE uw_gfm SET q = 1 WHERE ->uw_gfm_r[WHERE w <= [0].concat([lim]).max()] != [] RETURN VALUE id;
UPDATE us_gfm SET z = ->us_gfm_r[WHERE w <= [0].concat([lim]).max()] RETURN VALUE z;
UPDATE ur_gfm SET q = 1 RETURN VALUE ->ur_gfm_r[WHERE w <= [0].concat([lim]).max()];
DELETE dw_gfm WHERE ->dw_gfm_r[WHERE w <= [0].concat([lim]).max()] != [] RETURN VALUE $before.id;
UPSERT pw_gfm SET q = 1 WHERE ->pw_gfm_r[WHERE w <= [0].concat([lim]).max()] != [] RETURN VALUE id;
UPDATE fv_gfm SET q = 1 RETURN VALUE z;
UPDATE ev_gfm SET q = 1 RETURN VALUE id;
SELECT VALUE src FROM ev_gfm_log ORDER BY src;
UPDATE vw_gfm SET q = 1 RETURN NONE;
SELECT VALUE ws FROM vw_gfm_v ORDER BY id;
UPDATE uw_ofi SET q = 1 WHERE o[WHERE w <= [0, 8, 100][idx]] != [] RETURN VALUE id;
UPDATE us_ofi SET z = o[WHERE w <= [0, 8, 100][idx]] RETURN VALUE z;
UPDATE ur_ofi SET q = 1 RETURN VALUE o[WHERE w <= [0, 8, 100][idx]];
DELETE dw_ofi WHERE o[WHERE w <= [0, 8, 100][idx]] != [] RETURN VALUE $before.id;
UPSERT pw_ofi SET q = 1 WHERE o[WHERE w <= [0, 8, 100][idx]] != [] RETURN VALUE id;
UPDATE fv_ofi SET q = 1 RETURN VALUE z;
UPDATE ev_ofi SET q = 1 RETURN VALUE id;
SELECT VALUE src FROM ev_ofi_log ORDER BY src;
UPDATE vw_ofi SET q = 1 RETURN NONE;
SELECT VALUE ws FROM vw_ofi_v ORDER BY id;
UPDATE uw_ofm SET q = 1 WHERE o[WHERE w <= [0].concat([lim]).max()] != [] RETURN VALUE id;
UPDATE us_ofm SET z = o[WHERE w <= [0].concat([lim]).max()] RETURN VALUE z;
UPDATE ur_ofm SET q = 1 RETURN VALUE o[WHERE w <= [0].concat([lim]).max()];
DELETE dw_ofm WHERE o[WHERE w <= [0].concat([lim]).max()] != [] RETURN VALUE $before.id;
UPSERT pw_ofm SET q = 1 WHERE o[WHERE w <= [0].concat([lim]).max()] != [] RETURN VALUE id;
UPDATE fv_ofm SET q = 1 RETURN VALUE z;
UPDATE ev_ofm SET q = 1 RETURN VALUE id;
SELECT VALUE src FROM ev_ofm_log ORDER BY src;
UPDATE vw_ofm SET q = 1 RETURN NONE;
SELECT VALUE ws FROM vw_ofm_v ORDER BY id;
