/**
[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
Half of a pair. Same queries as
`group_projection_sequence_legacy.surql`, pinned to the streaming planner;
the two files' expected results are the divergence.

A grouped projection reduces an expression the row cannot influence to a
single value per group — see `group_projection_reduces_per_group.surql`,
where every strategy agrees. `sequence::nextval` cannot be read that way:
each call hands out a value no other call will, and the values it has
handed out outlive the statement, so the number of evaluations is itself
part of what the statement did. Reducing it would let the grouping decide
how many values a query consumes, and would consume one for a `GROUP ALL`
that matched no rows at all — which is what the legacy path does, since
`GROUP ALL` has its group whatever reaches it.

So it stays on the per-row path here: one group of three rows consumes
three values and reports all three, the next call continues from 3, and
the empty `GROUP ALL` collects no per-row value at all — an empty list,
with `t` still at its start. The legacy path reduces instead, reporting
one value and consuming one, for a group of three rows and for a group
of none alike.

Whether the reduced reading is the better one is a product call about
side effects rather than about values, and it is not the question this
pipeline's rule answers; the conservative direction is the one that
cannot consume a value nobody asked for.
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
