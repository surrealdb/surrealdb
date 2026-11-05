/**
[env]

[test]
reason = """
A grouped projection reduces an expression the row cannot influence to a single
value per group -- see `group_projection_reduces_per_group.surql`.
`sequence::nextval` cannot be read that way: each call hands out a value no
other call will, and the values it has handed out outlive the statement, so the
number of evaluations is itself part of what the statement did. Reducing it
would let the grouping decide how many values a query consumes, and would
consume one for a `GROUP ALL` that matched no rows at all.

So it is evaluated per row: one group of three rows consumes three values and
reports all three, the next call continues from 3, and the empty `GROUP ALL`
collects no per-row value at all -- an empty list, with `t` still at its start.
Whether a reduced reading would be better is a product call about side effects
rather than about values; the conservative direction is the one that cannot
consume a value nobody asked for.
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
value = "[{ g: 'a', k: [0, 1, 2] }]"

[[test.results]]
value = "3"

[[test.results]]
value = "[{ k: [] }]"

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

-- A GROUP ALL over a table with no rows. Both what the group reports and
-- whether a value was consumed to report it are the divergence here.
SELECT sequence::nextval('t') AS k FROM empty GROUP ALL;
RETURN sequence::nextval('t');
