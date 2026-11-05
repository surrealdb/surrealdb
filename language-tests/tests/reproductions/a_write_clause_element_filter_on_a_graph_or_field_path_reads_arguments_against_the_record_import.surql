/**
[test]
reason = "Setup for a_write_clause_element_filter_on_a_graph_or_field_path_reads_arguments_against_the_record: one table per position and shape, each with two rows holding an array field of objects and an edge between them, and the field, event or view definition the position holds (a view is defined before its rows, a field and an event after them). Runs as owner."
run = false
*/

DEFINE TABLE uw_gci SCHEMALESS;

CREATE uw_gci:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE uw_gci:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE uw_gci:1->uw_gci_r:1->uw_gci:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE us_gci SCHEMALESS;

CREATE us_gci:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE us_gci:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE us_gci:1->us_gci_r:1->us_gci:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE ur_gci SCHEMALESS;

CREATE ur_gci:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE ur_gci:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE ur_gci:1->ur_gci_r:1->ur_gci:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE dw_gci SCHEMALESS;

CREATE dw_gci:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE dw_gci:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE dw_gci:1->dw_gci_r:1->dw_gci:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE pw_gci SCHEMALESS;

CREATE pw_gci:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE pw_gci:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE pw_gci:1->pw_gci_r:1->pw_gci:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE fv_gci SCHEMALESS;

CREATE fv_gci:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE fv_gci:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE fv_gci:1->fv_gci_r:1->fv_gci:2 SET w = 3, lim = 2, idx = 0;
DEFINE FIELD z ON fv_gci VALUE ->fv_gci_r[WHERE array::any([w], |$f| $f <= [0, 8, 100][idx])];

DEFINE TABLE ev_gci SCHEMALESS;

CREATE ev_gci:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE ev_gci:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE ev_gci:1->ev_gci_r:1->ev_gci:2 SET w = 3, lim = 2, idx = 0;
DEFINE TABLE ev_gci_log SCHEMALESS; DEFINE EVENT e ON ev_gci WHEN ->ev_gci_r[WHERE array::any([w], |$f| $f <= [0, 8, 100][idx])] != [] THEN (CREATE ev_gci_log SET src = $value.id);

DEFINE TABLE vw_gci SCHEMALESS;
DEFINE TABLE vw_gci_v AS SELECT ->vw_gci_r[WHERE array::any([w], |$f| $f <= [0, 8, 100][idx])] AS ws, id FROM vw_gci;
CREATE vw_gci:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE vw_gci:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE vw_gci:1->vw_gci_r:1->vw_gci:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE uw_gcm SCHEMALESS;

CREATE uw_gcm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE uw_gcm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE uw_gcm:1->uw_gcm_r:1->uw_gcm:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE us_gcm SCHEMALESS;

CREATE us_gcm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE us_gcm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE us_gcm:1->us_gcm_r:1->us_gcm:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE ur_gcm SCHEMALESS;

CREATE ur_gcm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE ur_gcm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE ur_gcm:1->ur_gcm_r:1->ur_gcm:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE dw_gcm SCHEMALESS;

CREATE dw_gcm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE dw_gcm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE dw_gcm:1->dw_gcm_r:1->dw_gcm:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE pw_gcm SCHEMALESS;

CREATE pw_gcm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE pw_gcm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE pw_gcm:1->pw_gcm_r:1->pw_gcm:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE fv_gcm SCHEMALESS;

CREATE fv_gcm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE fv_gcm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE fv_gcm:1->fv_gcm_r:1->fv_gcm:2 SET w = 3, lim = 2, idx = 0;
DEFINE FIELD z ON fv_gcm VALUE ->fv_gcm_r[WHERE array::any([w], |$f| $f <= [0].concat([lim]).max())];

DEFINE TABLE ev_gcm SCHEMALESS;

CREATE ev_gcm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE ev_gcm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE ev_gcm:1->ev_gcm_r:1->ev_gcm:2 SET w = 3, lim = 2, idx = 0;
DEFINE TABLE ev_gcm_log SCHEMALESS; DEFINE EVENT e ON ev_gcm WHEN ->ev_gcm_r[WHERE array::any([w], |$f| $f <= [0].concat([lim]).max())] != [] THEN (CREATE ev_gcm_log SET src = $value.id);

DEFINE TABLE vw_gcm SCHEMALESS;
DEFINE TABLE vw_gcm_v AS SELECT ->vw_gcm_r[WHERE array::any([w], |$f| $f <= [0].concat([lim]).max())] AS ws, id FROM vw_gcm;
CREATE vw_gcm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE vw_gcm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE vw_gcm:1->vw_gcm_r:1->vw_gcm:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE uw_oci SCHEMALESS;

CREATE uw_oci:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE uw_oci:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE uw_oci:1->uw_oci_r:1->uw_oci:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE us_oci SCHEMALESS;

CREATE us_oci:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE us_oci:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE us_oci:1->us_oci_r:1->us_oci:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE ur_oci SCHEMALESS;

CREATE ur_oci:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE ur_oci:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE ur_oci:1->ur_oci_r:1->ur_oci:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE dw_oci SCHEMALESS;

CREATE dw_oci:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE dw_oci:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE dw_oci:1->dw_oci_r:1->dw_oci:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE pw_oci SCHEMALESS;

CREATE pw_oci:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE pw_oci:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE pw_oci:1->pw_oci_r:1->pw_oci:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE fv_oci SCHEMALESS;

CREATE fv_oci:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE fv_oci:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE fv_oci:1->fv_oci_r:1->fv_oci:2 SET w = 3, lim = 2, idx = 0;
DEFINE FIELD z ON fv_oci VALUE o[WHERE array::any([w], |$f| $f <= [0, 8, 100][idx])];

DEFINE TABLE ev_oci SCHEMALESS;

CREATE ev_oci:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE ev_oci:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE ev_oci:1->ev_oci_r:1->ev_oci:2 SET w = 3, lim = 2, idx = 0;
DEFINE TABLE ev_oci_log SCHEMALESS; DEFINE EVENT e ON ev_oci WHEN o[WHERE array::any([w], |$f| $f <= [0, 8, 100][idx])] != [] THEN (CREATE ev_oci_log SET src = $value.id);

DEFINE TABLE vw_oci SCHEMALESS;
DEFINE TABLE vw_oci_v AS SELECT o[WHERE array::any([w], |$f| $f <= [0, 8, 100][idx])] AS ws, id FROM vw_oci;
CREATE vw_oci:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE vw_oci:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE vw_oci:1->vw_oci_r:1->vw_oci:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE uw_ocm SCHEMALESS;

CREATE uw_ocm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE uw_ocm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE uw_ocm:1->uw_ocm_r:1->uw_ocm:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE us_ocm SCHEMALESS;

CREATE us_ocm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE us_ocm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE us_ocm:1->us_ocm_r:1->us_ocm:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE ur_ocm SCHEMALESS;

CREATE ur_ocm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE ur_ocm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE ur_ocm:1->ur_ocm_r:1->ur_ocm:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE dw_ocm SCHEMALESS;

CREATE dw_ocm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE dw_ocm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE dw_ocm:1->dw_ocm_r:1->dw_ocm:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE pw_ocm SCHEMALESS;

CREATE pw_ocm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE pw_ocm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE pw_ocm:1->pw_ocm_r:1->pw_ocm:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE fv_ocm SCHEMALESS;

CREATE fv_ocm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE fv_ocm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE fv_ocm:1->fv_ocm_r:1->fv_ocm:2 SET w = 3, lim = 2, idx = 0;
DEFINE FIELD z ON fv_ocm VALUE o[WHERE array::any([w], |$f| $f <= [0].concat([lim]).max())];

DEFINE TABLE ev_ocm SCHEMALESS;

CREATE ev_ocm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE ev_ocm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE ev_ocm:1->ev_ocm_r:1->ev_ocm:2 SET w = 3, lim = 2, idx = 0;
DEFINE TABLE ev_ocm_log SCHEMALESS; DEFINE EVENT e ON ev_ocm WHEN o[WHERE array::any([w], |$f| $f <= [0].concat([lim]).max())] != [] THEN (CREATE ev_ocm_log SET src = $value.id);

DEFINE TABLE vw_ocm SCHEMALESS;
DEFINE TABLE vw_ocm_v AS SELECT o[WHERE array::any([w], |$f| $f <= [0].concat([lim]).max())] AS ws, id FROM vw_ocm;
CREATE vw_ocm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE vw_ocm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE vw_ocm:1->vw_ocm_r:1->vw_ocm:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE uw_gfi SCHEMALESS;

CREATE uw_gfi:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE uw_gfi:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE uw_gfi:1->uw_gfi_r:1->uw_gfi:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE us_gfi SCHEMALESS;

CREATE us_gfi:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE us_gfi:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE us_gfi:1->us_gfi_r:1->us_gfi:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE ur_gfi SCHEMALESS;

CREATE ur_gfi:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE ur_gfi:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE ur_gfi:1->ur_gfi_r:1->ur_gfi:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE dw_gfi SCHEMALESS;

CREATE dw_gfi:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE dw_gfi:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE dw_gfi:1->dw_gfi_r:1->dw_gfi:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE pw_gfi SCHEMALESS;

CREATE pw_gfi:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE pw_gfi:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE pw_gfi:1->pw_gfi_r:1->pw_gfi:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE fv_gfi SCHEMALESS;

CREATE fv_gfi:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE fv_gfi:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE fv_gfi:1->fv_gfi_r:1->fv_gfi:2 SET w = 3, lim = 2, idx = 0;
DEFINE FIELD z ON fv_gfi VALUE ->fv_gfi_r[WHERE w <= [0, 8, 100][idx]];

DEFINE TABLE ev_gfi SCHEMALESS;

CREATE ev_gfi:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE ev_gfi:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE ev_gfi:1->ev_gfi_r:1->ev_gfi:2 SET w = 3, lim = 2, idx = 0;
DEFINE TABLE ev_gfi_log SCHEMALESS; DEFINE EVENT e ON ev_gfi WHEN ->ev_gfi_r[WHERE w <= [0, 8, 100][idx]] != [] THEN (CREATE ev_gfi_log SET src = $value.id);

DEFINE TABLE vw_gfi SCHEMALESS;
DEFINE TABLE vw_gfi_v AS SELECT ->vw_gfi_r[WHERE w <= [0, 8, 100][idx]] AS ws, id FROM vw_gfi;
CREATE vw_gfi:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE vw_gfi:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE vw_gfi:1->vw_gfi_r:1->vw_gfi:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE uw_gfm SCHEMALESS;

CREATE uw_gfm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE uw_gfm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE uw_gfm:1->uw_gfm_r:1->uw_gfm:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE us_gfm SCHEMALESS;

CREATE us_gfm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE us_gfm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE us_gfm:1->us_gfm_r:1->us_gfm:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE ur_gfm SCHEMALESS;

CREATE ur_gfm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE ur_gfm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE ur_gfm:1->ur_gfm_r:1->ur_gfm:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE dw_gfm SCHEMALESS;

CREATE dw_gfm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE dw_gfm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE dw_gfm:1->dw_gfm_r:1->dw_gfm:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE pw_gfm SCHEMALESS;

CREATE pw_gfm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE pw_gfm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE pw_gfm:1->pw_gfm_r:1->pw_gfm:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE fv_gfm SCHEMALESS;

CREATE fv_gfm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE fv_gfm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE fv_gfm:1->fv_gfm_r:1->fv_gfm:2 SET w = 3, lim = 2, idx = 0;
DEFINE FIELD z ON fv_gfm VALUE ->fv_gfm_r[WHERE w <= [0].concat([lim]).max()];

DEFINE TABLE ev_gfm SCHEMALESS;

CREATE ev_gfm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE ev_gfm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE ev_gfm:1->ev_gfm_r:1->ev_gfm:2 SET w = 3, lim = 2, idx = 0;
DEFINE TABLE ev_gfm_log SCHEMALESS; DEFINE EVENT e ON ev_gfm WHEN ->ev_gfm_r[WHERE w <= [0].concat([lim]).max()] != [] THEN (CREATE ev_gfm_log SET src = $value.id);

DEFINE TABLE vw_gfm SCHEMALESS;
DEFINE TABLE vw_gfm_v AS SELECT ->vw_gfm_r[WHERE w <= [0].concat([lim]).max()] AS ws, id FROM vw_gfm;
CREATE vw_gfm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE vw_gfm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE vw_gfm:1->vw_gfm_r:1->vw_gfm:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE uw_ofi SCHEMALESS;

CREATE uw_ofi:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE uw_ofi:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE uw_ofi:1->uw_ofi_r:1->uw_ofi:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE us_ofi SCHEMALESS;

CREATE us_ofi:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE us_ofi:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE us_ofi:1->us_ofi_r:1->us_ofi:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE ur_ofi SCHEMALESS;

CREATE ur_ofi:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE ur_ofi:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE ur_ofi:1->ur_ofi_r:1->ur_ofi:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE dw_ofi SCHEMALESS;

CREATE dw_ofi:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE dw_ofi:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE dw_ofi:1->dw_ofi_r:1->dw_ofi:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE pw_ofi SCHEMALESS;

CREATE pw_ofi:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE pw_ofi:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE pw_ofi:1->pw_ofi_r:1->pw_ofi:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE fv_ofi SCHEMALESS;

CREATE fv_ofi:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE fv_ofi:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE fv_ofi:1->fv_ofi_r:1->fv_ofi:2 SET w = 3, lim = 2, idx = 0;
DEFINE FIELD z ON fv_ofi VALUE o[WHERE w <= [0, 8, 100][idx]];

DEFINE TABLE ev_ofi SCHEMALESS;

CREATE ev_ofi:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE ev_ofi:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE ev_ofi:1->ev_ofi_r:1->ev_ofi:2 SET w = 3, lim = 2, idx = 0;
DEFINE TABLE ev_ofi_log SCHEMALESS; DEFINE EVENT e ON ev_ofi WHEN o[WHERE w <= [0, 8, 100][idx]] != [] THEN (CREATE ev_ofi_log SET src = $value.id);

DEFINE TABLE vw_ofi SCHEMALESS;
DEFINE TABLE vw_ofi_v AS SELECT o[WHERE w <= [0, 8, 100][idx]] AS ws, id FROM vw_ofi;
CREATE vw_ofi:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE vw_ofi:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE vw_ofi:1->vw_ofi_r:1->vw_ofi:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE uw_ofm SCHEMALESS;

CREATE uw_ofm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE uw_ofm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE uw_ofm:1->uw_ofm_r:1->uw_ofm:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE us_ofm SCHEMALESS;

CREATE us_ofm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE us_ofm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE us_ofm:1->us_ofm_r:1->us_ofm:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE ur_ofm SCHEMALESS;

CREATE ur_ofm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE ur_ofm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE ur_ofm:1->ur_ofm_r:1->ur_ofm:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE dw_ofm SCHEMALESS;

CREATE dw_ofm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE dw_ofm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE dw_ofm:1->dw_ofm_r:1->dw_ofm:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE pw_ofm SCHEMALESS;

CREATE pw_ofm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE pw_ofm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE pw_ofm:1->pw_ofm_r:1->pw_ofm:2 SET w = 3, lim = 2, idx = 0;


DEFINE TABLE fv_ofm SCHEMALESS;

CREATE fv_ofm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE fv_ofm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE fv_ofm:1->fv_ofm_r:1->fv_ofm:2 SET w = 3, lim = 2, idx = 0;
DEFINE FIELD z ON fv_ofm VALUE o[WHERE w <= [0].concat([lim]).max()];

DEFINE TABLE ev_ofm SCHEMALESS;

CREATE ev_ofm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE ev_ofm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE ev_ofm:1->ev_ofm_r:1->ev_ofm:2 SET w = 3, lim = 2, idx = 0;
DEFINE TABLE ev_ofm_log SCHEMALESS; DEFINE EVENT e ON ev_ofm WHEN o[WHERE w <= [0].concat([lim]).max()] != [] THEN (CREATE ev_ofm_log SET src = $value.id);

DEFINE TABLE vw_ofm SCHEMALESS;
DEFINE TABLE vw_ofm_v AS SELECT o[WHERE w <= [0].concat([lim]).max()] AS ws, id FROM vw_ofm;
CREATE vw_ofm:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE vw_ofm:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE vw_ofm:1->vw_ofm_r:1->vw_ofm:2 SET w = 3, lim = 2, idx = 0;

