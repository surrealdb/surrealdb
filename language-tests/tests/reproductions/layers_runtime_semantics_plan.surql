/**
[env]
imports = ["reproductions/layers_semantics_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	Plan companion to layers_runtime_semantics: over a table with
	guarded and computed fields, `SELECT *` and an aliased projection of the
	same fields stack the computed layer beneath the guard layer; a predicate
	on a re-derived field keeps the filter above the stack. A guard on a
	wildcard path is left to the scan — a stored declaration binds writes
	alone, so no expression over the column reproduces the cut the runtime
	makes — for the whole row, the projected field and the VALUE form alike.
	A FROM naming two tables
	plans one layered scan per table, each carrying that table's own guards.
"""

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    ProjectLayer [ctx: Db] [columns: g = <permission> @permission, c_pass = <permission> : string @permission, c_raw = <permission> : string @permission, c_reads = <computed> : bool @computed, c_self = <computed> : none | string @computed]
        ProjectLayer [ctx: Db] [columns: c_pass = <computed> : string @computed, c_raw = <computed> : string @computed]
            TableScan [ctx: Db] [table: item, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: stored, raw_judged, rederived, self_view, admitted]
    ProjectLayer [ctx: Db] [columns: g = <permission> @permission, c_pass = <permission> : string @permission, c_raw = <permission> : string @permission, c_reads = <computed> : bool @computed, c_self = <computed> : none | string @computed]
        ProjectLayer [ctx: Db] [columns: c_pass = <computed> : string @computed, c_raw = <computed> : string @computed]
            TableScan [ctx: Db] [table: item, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: c_reads = true]
        ProjectLayer [ctx: Db] [columns: g = <permission> @permission, c_pass = <permission> : string @permission, c_raw = <permission> : string @permission, c_reads = <computed> : bool @computed, c_self = <computed> : none | string @computed]
            ProjectLayer [ctx: Db] [columns: c_pass = <computed> : string @computed, c_raw = <computed> : string @computed]
                TableScan [ctx: Db] [table: item, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: bag, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: arr]
    TableScan [ctx: Db] [table: bag, direction: Forward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: arr]
    TableScan [ctx: Db] [table: bag, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Union [ctx: Db]
        Subquery [ctx: Rt]
            SelectProject [ctx: Db] [projections: *]
                ProjectLayer [ctx: Db] [columns: hid_a = <permission> @permission]
                    TableScan [ctx: Db] [table: a, direction: Forward]
        Subquery [ctx: Rt]
            SelectProject [ctx: Db] [projections: *]
                ProjectLayer [ctx: Db] [columns: hid_b = <permission> @permission]
                    TableScan [ctx: Db] [table: b, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: pub, ha, hb]
    Union [ctx: Db]
        Subquery [ctx: Rt]
            SelectProject [ctx: Db] [projections: *]
                ProjectLayer [ctx: Db] [columns: hid_a = <permission> @permission]
                    TableScan [ctx: Db] [table: a, direction: Forward]
        Subquery [ctx: Rt]
            SelectProject [ctx: Db] [projections: *]
                ProjectLayer [ctx: Db] [columns: hid_b = <permission> @permission]
                    TableScan [ctx: Db] [table: b, direction: Forward]
''''
*/

-- Denied fields are absent; computed fields carry the caller's view.
EXPLAIN SELECT * FROM item;
EXPLAIN SELECT g AS stored, c_raw AS raw_judged, c_reads AS rederived, c_self AS self_view, c_pass AS admitted FROM item;
EXPLAIN SELECT * FROM item WHERE c_reads = true;

-- A wildcard guard cuts the key from every element.
EXPLAIN SELECT * FROM bag;
EXPLAIN SELECT arr FROM bag;
EXPLAIN SELECT VALUE arr FROM bag;

-- Each table's guards apply to that table's rows.
EXPLAIN SELECT * FROM a, b;
EXPLAIN SELECT pub, hid_a AS ha, hid_b AS hb FROM a, b;
