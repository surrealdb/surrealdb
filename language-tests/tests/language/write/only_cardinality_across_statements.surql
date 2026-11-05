/**
[env]
namespace = true
database = true

[test]
reason = """
What `ONLY` does to a mutation's result when the target matches other than one
record. `INSERT` has no `ONLY`; the other five statements share one rule, and
it is applied to the rows the statement returns rather than to the rows it
touched.

More than one row fails with `Expected a single result output`, and that half
is checked for all five. No row is NONE rather than an error — the half most
easily inverted, since the keyword reads like a demand for exactly one. It is
checked here for UPDATE and DELETE, which are the two that can reach it:
`UPSERT ONLY` creates the record it did not find, and `CREATE ONLY` and
`RELATE ONLY` always produce a row or fail.

Because the rule counts returned rows, `DELETE ONLY` over two matching records
succeeds and yields NONE — its default is to return nothing, so nothing is what
gets counted. The same statement with `RETURN BEFORE` returns both records and
fails. Deleting two records through a keyword that promises one is worth seeing
written down.

`UPSERT` is given an array and a record-id range here rather than a bare table,
because a bare table makes it create a single new record with a generated id
instead of matching the two that exist.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: two:1, n: 1 }]"

[[test.results]]
value = "[{ id: two:2, n: 2 }]"

[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: del:1 }]"

[[test.results]]
value = "[{ id: del:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: del:1 }]"

[[test.results]]
value = "[{ id: del:2 }]"

[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ id: gone:3, n: 1 }"
*/

-- Two matching records: the statement fails rather than picking one. UPSERT is
-- given an array and a record-id range, because against a bare table it
-- creates a single new record instead of matching the existing two.
DEFINE TABLE two SCHEMALESS;
CREATE two:1 SET n = 1;
CREATE two:2 SET n = 2;
UPDATE ONLY two SET n = 9;
UPSERT ONLY [two:1, two:2] SET n = 9;
UPSERT ONLY two:1..=2 SET n = 8;
CREATE ONLY |mk:2|;
RELATE ONLY (SELECT * FROM two)->edge->two:1;

-- DELETE returns no rows unless asked for them, so two matching records still
-- satisfy ONLY; RETURN BEFORE surfaces the pair and fails.
DEFINE TABLE del SCHEMALESS;
CREATE del:1;
CREATE del:2;
DELETE ONLY del;
CREATE del:1;
CREATE del:2;
DELETE ONLY del RETURN BEFORE;

-- Zero matches is NONE, not an error — for a defined table with nothing in it,
-- and for a record id that does not exist on a defined table.
DEFINE TABLE empt SCHEMALESS;
UPDATE ONLY empt SET n = 1;
DELETE ONLY empt;
DEFINE TABLE gone SCHEMALESS;
UPDATE ONLY gone:1 SET n = 1;
DELETE ONLY gone:2;

-- UPSERT ONLY creates the record it did not find, so it returns that one
-- record rather than NONE.
UPSERT ONLY gone:3 SET n = 1;
