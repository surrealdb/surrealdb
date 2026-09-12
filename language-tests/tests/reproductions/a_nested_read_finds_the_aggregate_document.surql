/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
A post-expression is evaluated once per group against the aggregate document —
the finalized aggregates beside the group's keys — and that document is the
*root* of the evaluation, not merely the value the expression starts from. A
nested expression is evaluated against the root rather than against the
position the path has reached: a method's arguments read the document the
receiver came from, and so does a dynamic index. A key or a second aggregate
named from inside one therefore has to find the same document the receiver
was read from.

Each group answers from its own keys, which is what the multi-group case
checks: the base of the logarithm changes with the group.

`planner-strategy` excludes the per-row engine, which rejects an aggregate
anywhere but as a direct argument of the outer call — `math::log(math::sum(v),
g)` is the form it accepts, and that agrees on every strategy in
`a_grouped_key_reaches_the_post_expression`. The forms here, an
aggregate as a method receiver or inside an array literal, are streaming-only.
"""

[[test.results]]
value = "[{ g: 2, x: 3f }]"

[[test.results]]
value = "[{ g: 2, x: 3f }]"

[[test.results]]
value = "[{ g: 2, x: 3f }]"

[[test.results]]
value = "[{ g: 1, x: 3f }]"

[[test.results]]
value = "[{ g: 2, x: 3.1699250014423126f }, { g: 3, x: 2f }]"

[[test.results]]
value = "[{ g: 0, x: 8 }]"

[[test.results]]
value = "[{ g: 1, x: 99 }]"

[[test.results]]
value = "[{ g: 1, x: 8 }]"
*/

-- A grouped key read from inside a method's argument, beside the literal
-- argument that never needed the document at all.
SELECT math::sum(v).log(g) AS x, g FROM [{ v: 8, g: 2 }] GROUP BY g;
SELECT math::sum(v).log(2) AS x, g FROM [{ v: 8, g: 2 }] GROUP BY g;

-- An expression over the key, not only the bare key.
SELECT math::sum(v).log(g + 0) AS x, g FROM [{ v: 8, g: 2 }] GROUP BY g;

-- A second aggregate reaches the document from the same place.
SELECT math::sum(v).log(math::max(w)) AS x, g FROM [{ v: 8, g: 1, w: 2 }] GROUP BY g;

-- Each group reads its own key.
SELECT math::sum(v).log(g) AS x, g
FROM [{ v: 8, g: 2 }, { v: 9, g: 3 }, { v: 1, g: 2 }]
GROUP BY g ORDER BY g;

-- A dynamic index is the other nested read, and it picks the aggregate or the
-- literal beside it according to the group's key.
SELECT [math::sum(v), 99][g] AS x, g FROM [{ v: 8, g: 0 }] GROUP BY g;
SELECT [math::sum(v), 99][g] AS x, g FROM [{ v: 8, g: 1 }] GROUP BY g;
SELECT [99, math::sum(v)][g] AS x, g FROM [{ v: 8, g: 1 }] GROUP BY g;
