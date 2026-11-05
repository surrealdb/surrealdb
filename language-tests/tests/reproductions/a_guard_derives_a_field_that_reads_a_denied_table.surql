/**
[env]
namespace = true
database = true
imports = ["./a_guard_derives_a_field_that_reads_a_denied_table_import.surql", "./a_late_bound_statement_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:probe" }

[test]
reason = """
	A table permission that reads a computed field derives it unchecked, so
	a body reading a table the caller may not select still reads it: the
	flagged record is hidden from a scan, a record dereference and a FETCH
	alike, planned where it stands or late-bound. The caller's own read of
	the field derives it as the caller, whose read of that table is empty.
	A body reading under a LIMIT from a source the caller's permission
	empties runs rather than being refused.
"""

[[test.results]]
value = "[{ flagged: false, id: doc:pub, status: 'public' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ flagged: false, id: doc:pub, status: 'public' }"

[[test.results]]
value = "[{ d: NONE, e: { flagged: false, id: doc:pub, status: 'public' }, id: holder:1 }]"

[[test.results]]
value = "[{ f: NONE }]"

[[test.results]]
value = "[{ flagged: false, id: doc:pub, status: 'public' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ d: NONE, e: { flagged: false, id: doc:pub, status: 'public' }, id: holder:1 }]"
*/

SELECT * FROM doc;
RETURN doc:sec.*;
RETURN doc:pub.*;
SELECT * FROM holder FETCH d, e;
SELECT d.flagged AS f FROM holder;
{ LET $late = fn::late('now'); RETURN SELECT * FROM doc; };
{ LET $late = fn::late('now'); RETURN doc:sec.*; };
{ LET $late = fn::late('now'); RETURN SELECT * FROM holder FETCH d, e; };
