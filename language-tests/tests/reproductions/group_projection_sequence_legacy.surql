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

Both agree on the empty `GROUP ALL`, which consumes nothing — legacy
because it produces no row for it at all.
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
value = "NONE"

[[test.results]]
value = "0"
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

-- A GROUP ALL over a table with no rows. The select is bound rather than
-- returned because the two engines also disagree about the shape of an empty
-- GROUP ALL's output, which is pinned on its own elsewhere; what this asks is
-- only whether producing it consumed a sequence value.
LET $_ = (SELECT sequence::nextval('t') AS k FROM empty GROUP ALL);
RETURN sequence::nextval('t');
