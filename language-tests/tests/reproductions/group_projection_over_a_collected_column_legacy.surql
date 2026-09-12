/**
[env]
planner-strategy = ["compute-only"]

[test]
reason = """
Half of a pair. Same queries as
`group_projection_over_a_collected_column_streaming.surql`, pinned to the
legacy path; the two files' expected results are the divergence.

A column a grouped projection reads from the row is collected — `k` over two
rows reports `[1, 2]` on both engines. What differs is where an expression
around that column is applied. The per-row engine collects first and applies
the expression to the collection, so `k + 1` asks for an array plus an
integer and raises; the streaming engine applies it per row and collects the
results, reporting `[2, 3]`. The streaming reading is the intended one: the
expression is written against a row, and a group of one row would otherwise
answer differently from a group of two.

`GROUP ALL` over no rows reaches the same two paths, which is why it is here:
the collection is empty, and the per-row engine raises on `[] + 1` where the
streaming engine reports the empty collection. Neither engine invents a row
for it — the group exists either way — so this is the expression's divergence,
not the group's.

The bare column is the control both engines agree on, which is what places the
divergence in the expression rather than in the collecting.
"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot perform addition with 'array' and 'int'"

[[test.results]]
value = "[{ id: e:1, k: 1 }]"

[[test.results]]
value = "[{ id: e:2, k: 2 }]"

[[test.results]]
error = "Cannot perform addition with 'array' and 'int'"

[[test.results]]
value = "[{ bare: [1, 2], c: 2 }]"
*/

DEFINE TABLE e SCHEMALESS;

-- No row reaches the group, so the collection is empty.
SELECT k + 1 AS plus, count() AS c FROM e GROUP ALL;

CREATE e:1 SET k = 1;
CREATE e:2 SET k = 2;

-- Two rows, one group.
SELECT k + 1 AS plus, count() AS c FROM e GROUP ALL;

-- Control: the bare column collects the same way on both engines.
SELECT k AS bare, count() AS c FROM e GROUP ALL;
