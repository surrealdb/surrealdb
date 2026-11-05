/**
[env]
# The body asserts an `EXPLAIN` plan, and the ordinary language-test runner
# replays it as well as the upgrade axis.
imports = ["upgrade/define/3.00.00_id_components_onto_array_ids_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
version = ">=3.4.0-a"
reason = """
The reads in 3.00.00_id_components_onto_array_ids.surql return the same rows
whichever way they are served: a key range that narrowed to the wrong keys would
lose rows, but a fallback to a table scan returns exactly the right ones, so
they cannot tell a plan that reads the older writer's keys from one that never
reads them. This asserts the plan (a KeyRangeScan over the table's keys, with
the predicate kept above it), compares each range read with the same read under
WITH NOINDEX, which never takes a key range, and then writes, so this binary
maintains rows the older one wrote and adds rows of its own in the same second,
and one range reads both.

It is gated to this checkout as the upgrade target for the reason given in the
sibling: no released binary parses a named record id, so neither the pair axis
between releases nor the cumulative walk can run it, and `EXPLAIN` output is not
stable across the 3.x line. Every assertion holds whichever binary wrote the
import, because the ordinary language-test runner replays this file too and then
the import is written by this checkout."""

[[test.results]]
value = "NONE"

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "table: reading") AND string::contains($result, "key: city = 'London'") AND string::contains($result, "range: time >= d'2025-01-01T10:00:00") AND string::contains($result, "granularity: second") AND string::contains($result, "residual: yes") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[10, 11]"

[[test.results]]
value = "[10, 11]"

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "table: reading") AND string::contains($result, "key: city = 'Paris'") AND string::contains($result, "range: time > d'2025-01-01T10:00:00") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[21]"

[[test.results]]
value = "[21]"

[[test.results]]
value = "[15]"

[[test.results]]
value = "[16]"

[[test.results]]
value = "[11, 15, 16]"

[[test.results]]
value = "[11, 15, 16]"

[[test.results]]
value = "[25]"

[[test.results]]
value = "[{ city: 'Paris', id: reading:['Paris', d'2025-01-01T10:00:00Z'], temp: 25, time: d'2025-01-01T10:00:00Z' }]"

*/
-- Name the components of the ids the old binary wrote.
DEFINE FIELD OVERWRITE id ON reading TYPE [city: string, time: datetime];

-- A window on the datetime component beneath a pinned city is a range over the
-- keys the old binary wrote, and the predicate above it decides the rows.
EXPLAIN SELECT temp FROM reading WHERE city = 'London' AND time >= d'2025-01-01T10:00:00Z' AND time < d'2025-01-01T10:00:01Z';
SELECT VALUE temp FROM reading WHERE city = 'London' AND time >= d'2025-01-01T10:00:00Z' AND time < d'2025-01-01T10:00:01Z' ORDER BY temp;
-- The oracle: a table scan never takes a key range.
SELECT VALUE temp FROM reading WITH NOINDEX WHERE city = 'London' AND time >= d'2025-01-01T10:00:00Z' AND time < d'2025-01-01T10:00:01Z' ORDER BY temp;

-- A strict lower bound is a range too, and reaches the fraction of its second.
EXPLAIN SELECT temp FROM reading WHERE city = 'Paris' AND time > d'2025-01-01T10:00:00Z';
SELECT VALUE temp FROM reading WHERE city = 'Paris' AND time > d'2025-01-01T10:00:00Z' ORDER BY temp;
SELECT VALUE temp FROM reading WITH NOINDEX WHERE city = 'Paris' AND time > d'2025-01-01T10:00:00Z' ORDER BY temp;

-- A write of this binary maintains a row of the old one, found by the key the
-- components make, and a row it adds sits among the old ones in the same second.
UPSERT reading SET city = 'London', time = d'2025-01-01T10:00:00Z', temp = 15 RETURN VALUE temp;
UPSERT reading SET city = 'London', time = d'2025-01-01T10:00:00.750Z', temp = 16 RETURN VALUE temp;
-- One range reads the old rows and the new one.
SELECT VALUE temp FROM reading WHERE city = 'London' AND time >= d'2025-01-01T10:00:00Z' AND time < d'2025-01-01T10:00:01Z' ORDER BY temp;
SELECT VALUE temp FROM reading WITH NOINDEX WHERE city = 'London' AND time >= d'2025-01-01T10:00:00Z' AND time < d'2025-01-01T10:00:01Z' ORDER BY temp;

-- A write to the row that stored a copy of its city leaves the component as the key has it.
UPSERT reading:['Paris', d'2025-01-01T10:00:00Z'] SET temp = 25 RETURN VALUE temp;
SELECT * FROM reading:['Paris', d'2025-01-01T10:00:00Z'];
