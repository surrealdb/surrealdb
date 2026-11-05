/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A table whose `id` field has a tuple type with no component names keeps
	the plans it has always had: a predicate on an element of the id is judged
	over a table scan, never a key range, and an ordering by `id` is the table
	scan's own order, with no sort.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
match = '''string::contains($result, "TableScan [ctx: Db] [table: plain, direction: Forward]") AND string::contains($result, "Filter") AND !string::contains($result, "KeyRangeScan")'''
error = false

[[test.results]]
value = "[1, 2]"

[[test.results]]
match = '''string::contains($result, "TableScan [ctx: Db] [table: plain, direction: Forward]") AND !string::contains($result, "Sort [") AND !string::contains($result, "KeyRangeScan")'''
error = false

[[test.results]]
value = "[1, 2, 3]"
*/

DEFINE TABLE plain SCHEMALESS;
DEFINE FIELD id ON plain TYPE [string, datetime];
CREATE plain:['London', d'2024-01-01T00:00:00Z'] SET temp = 1 RETURN NONE;
CREATE plain:['London', d'2024-01-01T00:00:01Z'] SET temp = 2 RETURN NONE;
CREATE plain:['Paris', d'2024-01-01T00:00:00Z'] SET temp = 3 RETURN NONE;
EXPLAIN SELECT temp FROM plain WHERE id[0] = 'London';
SELECT VALUE temp FROM plain WHERE id[0] = 'London' ORDER BY temp;
EXPLAIN SELECT temp FROM plain ORDER BY id;
SELECT VALUE temp FROM plain ORDER BY id;
