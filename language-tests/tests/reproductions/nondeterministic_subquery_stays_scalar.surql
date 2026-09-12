/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = "decorrelation builds the subquery's relation once for every outer row, so a right side that answers differently each time stays a scalar subquery"

[[test.results]]
value = "[{ id: o:1, k: 1 }]"

[[test.results]]
value = "[{ id: r:1, k: 0 }]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: k]
    TableScan [ctx: Db] [table: o, direction: Forward, predicate: k INSIDE (ProjectValue), pre_decode_filter: no (unsupported predicate)]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: k]
    TableScan [ctx: Db] [table: o, direction: Forward, predicate: k INSIDE (ProjectValue), pre_decode_filter: no (unsupported predicate)]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: k]
    HashJoin [ctx: Db] [type: Semi, alias: _join0, on: k = _join0.k + 1]
        TableScan [ctx: Db] [table: o, direction: Forward]
        TableScan [ctx: Db] [table: r, direction: Forward]
''''
*/

-- Evaluated as written, `k IN (SELECT VALUE k FROM r WHERE …)` runs the
-- subquery once per outer row. Lifted into a semi join, the right side is
-- built once and every outer row probes that one relation. The two agree
-- only where the right side answers the same each time, so a `rand::` call
-- in the subquery's filter or in its `VALUE` keeps the scalar form, while
-- the same shape without one still lifts.

CREATE o:1 SET k = 1;
CREATE r:1 SET k = 0;

EXPLAIN SELECT VALUE k FROM o WHERE k IN (SELECT VALUE k FROM r WHERE rand::bool());

EXPLAIN SELECT VALUE k FROM o WHERE k IN (SELECT VALUE k FROM r WHERE k > rand::int(0, 2));

EXPLAIN SELECT VALUE k FROM o WHERE k IN (SELECT VALUE k + 1 FROM r);
