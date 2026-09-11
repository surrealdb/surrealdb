/**
[env]
planner-strategy = ["compute-only"]

[test]
reason = """
Half of a pair. Same queries as
`group_empty_set_aggregate_streaming.surql`, pinned to the legacy compute
path; the two files' expected results are the divergence.

`GROUP ALL` is one group over the whole input, so it produces one row even
when no input row reaches it. Legacy only does that for a lone `count()`:
add a second aggregate and the row disappears entirely, so the caller gets
`[]` where the streaming engine returns the zero row. The same `[]` appears
for `math::mean` and `math::stddev`, which the streaming engine answers with
`NaN`, and for a non-empty table that a WHERE narrows to nothing.

Legacy also refuses an aggregate over a field no row carries: `math::sum` and
`time::min` receive `NONE` and raise an argument-type error, where the
streaming engine returns the identity (`0`) or `NONE`.

The streaming answers look like the intended contract on every case here, so
these are gaps in the path being replaced and go away with it. Recorded so the
difference is visible while both engines are live, and so a change to the
streaming side is not mistaken for parity work.

Two controls bound the claim: a lone `count()` under `GROUP ALL` agrees, which
is what shows the legacy row-dropping is specific to a second aggregate rather
than to empty input; and `GROUP BY` over an empty table agrees at `[]`,
because no group key exists to emit.

`SELECT *, count() ... GROUP BY` is deliberately absent: both engines reject
it, differing only in that the streaming message carries an `Invalid query: `
prefix, which is a message difference rather than a behavioural one.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ n: 0 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ c: 'a', id: d:1, v: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function math::sum(). Argument 1 was the wrong type. Expected `number` but found `NONE`"

[[test.results]]
error = "Incorrect arguments for function time::min(). Argument 1 was the wrong type. Expected `datetime` but found `NONE`"
*/

DEFINE TABLE e SCHEMAFULL;
DEFINE FIELD c ON e TYPE string;
DEFINE FIELD v ON e TYPE int;

-- Control: a lone count() under GROUP ALL agrees across engines.
SELECT count() AS n FROM e GROUP ALL;

-- A second aggregate alongside it, over the same empty input.
SELECT count() AS n, math::sum(v) AS total FROM e GROUP ALL;
SELECT math::mean(v) AS avg FROM e GROUP ALL;
SELECT VALUE math::stddev(v) FROM e GROUP ALL;

-- Control: GROUP BY over an empty table has no group key to emit.
SELECT c, count() AS n FROM e GROUP BY c;

-- A populated table narrowed to nothing by the WHERE, rather than empty.
CREATE d:1 SET c = 'a', v = 1;
SELECT count() AS n FROM d WHERE v > 100 GROUP ALL;

-- An aggregate over a field no row carries.
SELECT c, math::sum(nope) AS total FROM d GROUP BY c;
SELECT c, time::min(nope) AS t FROM d GROUP BY c;
