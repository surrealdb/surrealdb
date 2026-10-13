/**
[test]
reason = "Setup for a_closure_as_a_step_argument_in_an_element_filter_reads_the_row_in_a_projection: a table per statement with two rows holding an array field of objects and an edge between them. Runs as owner."
run = false
*/

DEFINE TABLE sl_gmc SCHEMALESS;
CREATE sl_gmc:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE sl_gmc:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE sl_gmc:1->sl_gmc_r:1->sl_gmc:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE sl_gmt SCHEMALESS;
CREATE sl_gmt:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE sl_gmt:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE sl_gmt:1->sl_gmt_r:1->sl_gmt:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE us_gmt SCHEMALESS;
CREATE us_gmt:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE us_gmt:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE us_gmt:1->us_gmt_r:1->us_gmt:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE uw_gmt SCHEMALESS;
CREATE uw_gmt:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE uw_gmt:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE uw_gmt:1->uw_gmt_r:1->uw_gmt:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE pw_gmt SCHEMALESS;
CREATE pw_gmt:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE pw_gmt:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE pw_gmt:1->pw_gmt_r:1->pw_gmt:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE dw_gmt SCHEMALESS;
CREATE dw_gmt:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE dw_gmt:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE dw_gmt:1->dw_gmt_r:1->dw_gmt:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE sl_gic SCHEMALESS;
CREATE sl_gic:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE sl_gic:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE sl_gic:1->sl_gic_r:1->sl_gic:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE sl_git SCHEMALESS;
CREATE sl_git:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE sl_git:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE sl_git:1->sl_git_r:1->sl_git:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE us_git SCHEMALESS;
CREATE us_git:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE us_git:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE us_git:1->us_git_r:1->us_git:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE uw_git SCHEMALESS;
CREATE uw_git:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE uw_git:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE uw_git:1->uw_git_r:1->uw_git:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE pw_git SCHEMALESS;
CREATE pw_git:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE pw_git:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE pw_git:1->pw_git_r:1->pw_git:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE dw_git SCHEMALESS;
CREATE dw_git:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE dw_git:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE dw_git:1->dw_git_r:1->dw_git:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE sl_nmc SCHEMALESS;
CREATE sl_nmc:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE sl_nmc:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE sl_nmc:1->sl_nmc_r:1->sl_nmc:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE sl_nmt SCHEMALESS;
CREATE sl_nmt:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE sl_nmt:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE sl_nmt:1->sl_nmt_r:1->sl_nmt:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE us_nmt SCHEMALESS;
CREATE us_nmt:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE us_nmt:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE us_nmt:1->us_nmt_r:1->us_nmt:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE uw_nmt SCHEMALESS;
CREATE uw_nmt:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE uw_nmt:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE uw_nmt:1->uw_nmt_r:1->uw_nmt:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE pw_nmt SCHEMALESS;
CREATE pw_nmt:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE pw_nmt:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE pw_nmt:1->pw_nmt_r:1->pw_nmt:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE dw_nmt SCHEMALESS;
CREATE dw_nmt:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE dw_nmt:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE dw_nmt:1->dw_nmt_r:1->dw_nmt:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE sl_nic SCHEMALESS;
CREATE sl_nic:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE sl_nic:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE sl_nic:1->sl_nic_r:1->sl_nic:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE sl_nit SCHEMALESS;
CREATE sl_nit:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE sl_nit:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE sl_nit:1->sl_nit_r:1->sl_nit:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE us_nit SCHEMALESS;
CREATE us_nit:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE us_nit:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE us_nit:1->us_nit_r:1->us_nit:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE uw_nit SCHEMALESS;
CREATE uw_nit:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE uw_nit:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE uw_nit:1->uw_nit_r:1->uw_nit:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE pw_nit SCHEMALESS;
CREATE pw_nit:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE pw_nit:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE pw_nit:1->pw_nit_r:1->pw_nit:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE dw_nit SCHEMALESS;
CREATE dw_nit:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE dw_nit:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE dw_nit:1->dw_nit_r:1->dw_nit:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE sl_omc SCHEMALESS;
CREATE sl_omc:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE sl_omc:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE sl_omc:1->sl_omc_r:1->sl_omc:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE sl_omt SCHEMALESS;
CREATE sl_omt:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE sl_omt:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE sl_omt:1->sl_omt_r:1->sl_omt:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE us_omt SCHEMALESS;
CREATE us_omt:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE us_omt:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE us_omt:1->us_omt_r:1->us_omt:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE uw_omt SCHEMALESS;
CREATE uw_omt:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE uw_omt:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE uw_omt:1->uw_omt_r:1->uw_omt:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE pw_omt SCHEMALESS;
CREATE pw_omt:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE pw_omt:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE pw_omt:1->pw_omt_r:1->pw_omt:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE dw_omt SCHEMALESS;
CREATE dw_omt:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE dw_omt:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE dw_omt:1->dw_omt_r:1->dw_omt:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE sl_oic SCHEMALESS;
CREATE sl_oic:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE sl_oic:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE sl_oic:1->sl_oic_r:1->sl_oic:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE sl_oit SCHEMALESS;
CREATE sl_oit:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE sl_oit:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE sl_oit:1->sl_oit_r:1->sl_oit:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE us_oit SCHEMALESS;
CREATE us_oit:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE us_oit:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE us_oit:1->us_oit_r:1->us_oit:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE uw_oit SCHEMALESS;
CREATE uw_oit:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE uw_oit:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE uw_oit:1->uw_oit_r:1->uw_oit:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE pw_oit SCHEMALESS;
CREATE pw_oit:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE pw_oit:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE pw_oit:1->pw_oit_r:1->pw_oit:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE dw_oit SCHEMALESS;
CREATE dw_oit:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE dw_oit:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE dw_oit:1->dw_oit_r:1->dw_oit:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE sl_lmc SCHEMALESS;
CREATE sl_lmc:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE sl_lmc:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE sl_lmc:1->sl_lmc_r:1->sl_lmc:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE sl_lmt SCHEMALESS;
CREATE sl_lmt:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE sl_lmt:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE sl_lmt:1->sl_lmt_r:1->sl_lmt:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE us_lmt SCHEMALESS;
CREATE us_lmt:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE us_lmt:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE us_lmt:1->us_lmt_r:1->us_lmt:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE uw_lmt SCHEMALESS;
CREATE uw_lmt:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE uw_lmt:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE uw_lmt:1->uw_lmt_r:1->uw_lmt:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE pw_lmt SCHEMALESS;
CREATE pw_lmt:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE pw_lmt:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE pw_lmt:1->pw_lmt_r:1->pw_lmt:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE dw_lmt SCHEMALESS;
CREATE dw_lmt:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE dw_lmt:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE dw_lmt:1->dw_lmt_r:1->dw_lmt:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE sl_lic SCHEMALESS;
CREATE sl_lic:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE sl_lic:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE sl_lic:1->sl_lic_r:1->sl_lic:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE sl_lit SCHEMALESS;
CREATE sl_lit:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE sl_lit:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE sl_lit:1->sl_lit_r:1->sl_lit:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE us_lit SCHEMALESS;
CREATE us_lit:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE us_lit:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE us_lit:1->us_lit_r:1->us_lit:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE uw_lit SCHEMALESS;
CREATE uw_lit:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE uw_lit:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE uw_lit:1->uw_lit_r:1->uw_lit:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE pw_lit SCHEMALESS;
CREATE pw_lit:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE pw_lit:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE pw_lit:1->pw_lit_r:1->pw_lit:2 SET w = 3, lim = 2, idx = 0;

DEFINE TABLE dw_lit SCHEMALESS;
CREATE dw_lit:1 SET lim = 8, idx = 1, n = 1, o = [{ w: 3, lim: 2, idx: 0 }];
CREATE dw_lit:2 SET lim = 1, idx = 2, n = 10, o = [{ w: 3, lim: 2, idx: 0 }];
RELATE dw_lit:1->dw_lit_r:1->dw_lit:2 SET w = 3, lim = 2, idx = 0;
