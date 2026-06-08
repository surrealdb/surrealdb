/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = "Multi-clause same-field fusion: WHERE n > 3 AND n < 7 (and other range-shaped predicates) share a single map lookup per row. Multiple ops on one key are evaluated against the same `value_bytes` after one descent. EXPLAIN must still report pre_decode_filter: yes."

[[test.results]]
value = "{ b: false, id: pfrng:1, n: 1 }"

[[test.results]]
value = "{ b: false, id: pfrng:2, n: 4 }"

[[test.results]]
value = "{ b: true, id: pfrng:3, n: 5 }"

[[test.results]]
value = "{ b: false, id: pfrng:4, n: 6 }"

[[test.results]]
value = "{ b: true, id: pfrng:5, n: 9 }"

# WHERE n > 3 AND n < 7 → 4, 5, 6 (in id order)
[[test.results]]
value = "[{ id: pfrng:2 }, { id: pfrng:3 }, { id: pfrng:4 }]"

# WHERE n >= 4 AND n <= 6 → 4, 5, 6
[[test.results]]
value = "[{ id: pfrng:2 }, { id: pfrng:3 }, { id: pfrng:4 }]"

# WHERE n > 3 AND n < 7 AND n != 5 → 4, 6
[[test.results]]
value = "[{ id: pfrng:2 }, { id: pfrng:4 }]"

# Cross-field fusion still works: n > 3 AND n < 7 AND b = true
[[test.results]]
value = "[{ id: pfrng:3 }]"

# EXPLAIN: range query must show pre_decode_filter: yes
[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: pfrng, direction: Forward, predicate: n > 3 AND n < 7, pre_decode_filter: yes]
"'''

# EXPLAIN: mixed range + tag-only across multiple fields
[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: pfrng, direction: Forward, predicate: n > 3 AND n < 7 AND b = true, pre_decode_filter: yes]
"'''

*/

CREATE ONLY pfrng:1 SET n = 1, b = false;
CREATE ONLY pfrng:2 SET n = 4, b = false;
CREATE ONLY pfrng:3 SET n = 5, b = true;
CREATE ONLY pfrng:4 SET n = 6, b = false;
CREATE ONLY pfrng:5 SET n = 9, b = true;

SELECT id FROM pfrng WHERE n > 3 AND n < 7;
SELECT id FROM pfrng WHERE n >= 4 AND n <= 6;
SELECT id FROM pfrng WHERE n > 3 AND n < 7 AND n != 5;
SELECT id FROM pfrng WHERE n > 3 AND n < 7 AND b = true;

EXPLAIN SELECT id FROM pfrng WITH NOINDEX WHERE n > 3 AND n < 7;
EXPLAIN SELECT id FROM pfrng WITH NOINDEX WHERE n > 3 AND n < 7 AND b = true;
