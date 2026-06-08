/**
[test]
reason = "Setup for the non-graph counterpart of graph_perm_predicate_traversal. Several `doc` tables whose SELECT permission uses a SUBQUERY over a restricted source, exercising each scan-pipeline permission gate *inside* a permission predicate: `doc_tbl` scans a source with table-level NONE (the `check_perm!` / whole-scan `Deny` path), `doc_fld` projects a field with field-level NONE (the `filter_fields_by_permission` path), `doc_rid` scans by record id (the record_id-scan `Deny` path), and `doc_idx` scans via an index (the index-scan `Deny` path). Every predicate must evaluate unrestricted so a granted user can read the doc."
run = false
*/

-- (a) table-level NONE source -> exercises the table-permission gate.
DEFINE TABLE doc_tbl SCHEMALESS
	PERMISSIONS FOR select
		WHERE $auth.id IN (SELECT VALUE who FROM grant_tbl);
DEFINE TABLE grant_tbl SCHEMALESS;  -- no PERMISSIONS clause -> default NONE

-- (b) field-level NONE source -> exercises the field-permission gate. The
-- table is readable, but the projected `who` field denies select.
DEFINE TABLE doc_fld SCHEMALESS
	PERMISSIONS FOR select
		WHERE $auth.id IN (SELECT VALUE who FROM grant_fld);
DEFINE TABLE grant_fld SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE FIELD who ON grant_fld TYPE record<user> PERMISSIONS FOR select NONE;

-- (c) record-id scan over a table-level NONE source -> exercises the
-- record_id-scan gate inside a predicate.
DEFINE TABLE doc_rid SCHEMALESS
	PERMISSIONS FOR select
		WHERE $auth.id IN (SELECT VALUE who FROM grant_rid:g1);
DEFINE TABLE grant_rid SCHEMALESS;  -- default NONE

-- (d) indexed scan over a table-level NONE source -> exercises the
-- index-scan gate inside a predicate.
DEFINE TABLE doc_idx SCHEMALESS
	PERMISSIONS FOR select
		WHERE $auth.id IN (SELECT VALUE who FROM grant_idx WHERE who = $auth.id);
DEFINE TABLE grant_idx SCHEMALESS;  -- default NONE
DEFINE INDEX grant_idx_who ON grant_idx FIELDS who;

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );

CREATE user:martin, user:other;
CREATE doc_tbl:d1;
CREATE doc_fld:d1;
CREATE doc_rid:d1;
CREATE doc_idx:d1;
CREATE grant_tbl:g1 SET who = user:martin;
CREATE grant_fld:g1 SET who = user:martin;
CREATE grant_rid:g1 SET who = user:martin;
CREATE grant_idx:gi SET who = user:martin;
