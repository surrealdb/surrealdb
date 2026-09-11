/**
[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
Half of a pair. Same queries as
`group_empty_set_aggregate_legacy.surql`, pinned to the streaming planner; the
two files' expected results are the divergence.

`GROUP ALL` is one group over the whole input, so it produces one row even when
no input row reaches it. The streaming engine does that consistently: the zero
row for `count()` plus `math::sum`, `NaN` for `math::mean` and `math::stddev`,
and the zero row for a table a WHERE narrows to nothing. It also answers an
aggregate over a field no row carries with the identity (`0`) or `NONE`.

Legacy emits `[]` for each of those and raises an argument-type error from
`math::sum`/`time::min` when the field is absent, so the answers here look like
the intended contract and the divergence is a gap in the path being replaced.
Recorded so the difference is visible while both engines are live.

Two controls bound the claim: a lone `count()` under `GROUP ALL` agrees, which
shows the legacy row-dropping is specific to a second aggregate rather than to
empty input; and `GROUP BY` over an empty table agrees at `[]`, because no
group key exists to emit.

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
value = "[{ n: 0, total: 0 }]"

[[test.results]]
value = "[{ avg: NaN }]"

[[test.results]]
value = "[NaN]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ c: 'a', id: d:1, v: 1 }]"

[[test.results]]
value = "[{ n: 0 }]"

[[test.results]]
value = "[{ c: 'a', total: 0 }]"

[[test.results]]
value = "[{ c: 'a', t: NONE }]"
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
