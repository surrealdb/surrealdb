/**
[env]

[test]
reason = """
A column a grouped projection reads from the row is collected -- `k` over two
rows reports `[1, 2]`. An expression around that column is applied per row and
the results are collected, so `k + 1` reports `[2, 3]`. The expression is
written against a row, and a group of one row would otherwise answer
differently from a group of two.

`GROUP ALL` over no rows reaches the same path: the collection is empty and the
projection reports the empty collection. No row is invented for it -- the group
exists either way.

The bare column is the control that places the behaviour in the expression
rather than in the collecting.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 0, plus: [] }]"

[[test.results]]
value = "[{ id: e:1, k: 1 }]"

[[test.results]]
value = "[{ id: e:2, k: 2 }]"

[[test.results]]
value = "[{ c: 2, plus: [2, 3] }]"

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

-- Control: the bare column collects.
SELECT k AS bare, count() AS c FROM e GROUP ALL;
