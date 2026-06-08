/**
[env]
namespace = true
database = true

[test]
reason = """
Iterator::prepare handles `FROM <record-id>-><lookup>` specially when both
parts are present, but only ingested when the lookup had no filter clauses
(cond, limit, etc). When the lookup carried a filter (e.g. `SELECT * FROM
person:1->(knows WHERE since > 2020)`), the match arm silently exited without
calling `prepare_lookup` *or* `prepare_computed`, leaving the iterator empty
— the SELECT returned `[]` instead of the matching edges. `prepare_array`
already handled the same shape correctly via an `else => prepare_computed`
branch; `prepare` now matches that behaviour.
"""

[[test.results]]
value = "[{ id: person:1, name: 'Alice' }]"

[[test.results]]
value = "[{ id: person:2, name: 'Bob' }]"

[[test.results]]
value = "[{ id: person:3, name: 'Carol' }]"

[[test.results]]
value = "[{ id: knows:x, in: person:1, out: person:2, since: 2024 }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: knows:x, in: person:1, out: person:3, since: 2010 }]"
skip-record-id-key = true

[[test.results]]
value = "[2010, 2024]"

[[test.results]]
value = "[2024]"

[[test.results]]
value = "[2010]"

[[test.results]]
value = "[2024]"
*/

CREATE person:1 SET name = "Alice";
CREATE person:2 SET name = "Bob";
CREATE person:3 SET name = "Carol";
RELATE person:1->knows->person:2 SET since = 2024;
RELATE person:1->knows->person:3 SET since = 2010;

-- No filter — already worked via the `prepare_lookup` fast-path.
SELECT VALUE since FROM person:1->knows ORDER BY since;

-- With a WHERE filter inside the lookup. Before the fix this returned
-- `[]` because `Iterator::prepare` fell through without ingesting.
SELECT VALUE since FROM person:1->(knows WHERE since > 2020);

-- Different WHERE comparison — same code path, different filter.
SELECT VALUE since FROM person:1->(knows WHERE since < 2020);

-- With an AS alias inside the lookup — also hits the fall-through bug.
SELECT VALUE since FROM person:1->(knows WHERE since = 2024 AS recent);
