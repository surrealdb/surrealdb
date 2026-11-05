/**
[env]
imports = ["reproductions/udf_frame_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	Plan companion to udf_frame_write: a projection that dereferences
	a record link plans the dereference as an expression above the scan of
	the linking table; the linked table's `select` guard, and the write its
	function body reaches, belong to the dereference when the statement runs
	and do not appear as operators in the plan, so EXPLAIN reaches no guard
	and refuses nothing. A `SELECT *` over the readable audit table plans as
	a plain scan.
"""

[[test.results]]
value = '''
'Project [ctx: Db]
    TableScan [ctx: Db] [table: pub, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: audit, direction: Forward]
''''
*/

-- 0: the dereference reaches the guard, which reaches the write.
EXPLAIN SELECT link.name FROM pub;

-- 1: `audit:reader` must be absent.
EXPLAIN SELECT * FROM audit;
