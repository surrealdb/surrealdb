/**
[env]
namespace = true
database = true
auth = { level = "owner" }
timeout = 10000
context-timeout = 10000

[test]
reason = """
The plans behind the answers pinned in
`a_transaction_reads_around_an_index_it_defined.surql` and
`an_index_defined_over_an_empty_table_is_read_by_its_transaction.surql`. An
index a transaction defined is chosen by the planner only where the
transaction can read what its build wrote: not over rows committed before
the definition, whether the build ran inside the transaction or waits for it
to close, but at once over a table with no rows, and by every transaction
once it commits.
"""

[[test.results]]
value = "NONE"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: item, direction: Forward, predicate: k = 1, pre_decode_filter: yes]
''''

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: item, index: by_k, access: = 1, direction: Forward]
''''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: empty, index: by_k, access: = 1, direction: Forward]
''''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: late, direction: Forward, predicate: k = 1, pre_decode_filter: yes]
''''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/

DEFINE TABLE item SCHEMALESS;
DEFINE TABLE empty SCHEMALESS;
DEFINE TABLE late SCHEMALESS;
CREATE item:1 SET k = 1 RETURN NONE;
CREATE item:2 SET k = 2 RETURN NONE;
CREATE late:1 SET k = 1 RETURN NONE;

-- Over rows committed before it, a blocking index is not read by the
-- transaction that defined it, and is once that transaction commits.
BEGIN;
DEFINE INDEX by_k ON item FIELDS k;
EXPLAIN SELECT VALUE id FROM item WHERE k = 1;
COMMIT;
EXPLAIN SELECT VALUE id FROM item WHERE k = 1;

-- Over a table with no rows it is read at once.
BEGIN;
DEFINE INDEX by_k ON empty FIELDS k;
EXPLAIN SELECT VALUE id FROM empty WHERE k = 1;
COMMIT;

-- A CONCURRENTLY index holds nothing until its transaction has closed.
BEGIN;
DEFINE INDEX by_k ON late FIELDS k CONCURRENTLY;
EXPLAIN SELECT VALUE id FROM late WHERE k = 1;
COMMIT;

-- The build that waits for the transaction runs after it, so it is let finish.
SLEEP 1s;
