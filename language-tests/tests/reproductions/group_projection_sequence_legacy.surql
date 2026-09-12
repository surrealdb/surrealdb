/**
[env]
planner-strategy = ["compute-only"]

[test]
reason = """
Half of a pair. Same queries as
`group_projection_sequence_streaming.surql`, pinned to the legacy
`compute()` path; the two files' expected results are the divergence.

Here the projection is evaluated once per output row, which for a grouped
query means once per group: the group of three rows consumes a single
sequence value and reports it, so the next call continues from 1. The
streaming path evaluates it per row instead, because how many values a
statement consumes must not follow from how its rows grouped. The
reasoning is in the streaming half.

The empty `GROUP ALL` follows from the same reduction: `GROUP ALL` has
its group whether or not a row reaches it, so legacy evaluates the
projection once for that group and consumes a value for it. The streaming
path collects a per-row value instead, and an empty group collected none.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ g: 'a', id: items:1 }]"

[[test.results]]
value = "[{ g: 'a', id: items:2 }]"

[[test.results]]
value = "[{ g: 'a', id: items:3 }]"

[[test.results]]
value = "[{ g: 'a', k: 0 }]"

[[test.results]]
value = "1"

[[test.results]]
value = "[{ k: 0 }]"

[[test.results]]
value = "1"
*/

DEFINE SEQUENCE s BATCH 1 START 0;
DEFINE SEQUENCE t BATCH 1 START 0;
DEFINE TABLE empty SCHEMALESS;
CREATE items:1 SET g = 'a';
CREATE items:2 SET g = 'a';
CREATE items:3 SET g = 'a';

-- One group, three rows. What the group reports, and what the sequence has
-- handed out by the time the next call runs, say together how many values the
-- projection consumed.
SELECT sequence::nextval('s') AS k, g FROM items GROUP BY g;
RETURN sequence::nextval('s');

-- A GROUP ALL over a table with no rows. Both what the group reports and
-- whether a value was consumed to report it are the divergence here.
SELECT sequence::nextval('t') AS k FROM empty GROUP ALL;
RETURN sequence::nextval('t');
