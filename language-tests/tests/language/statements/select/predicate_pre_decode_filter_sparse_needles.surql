/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = "Wide rows + sparse needle list: the fused-flat scan switches from `entries()` iteration to per-needle `find_value_bytes` (O(M·log N)) when the predicate touches few fields against a wide object. Behaviour must stay correct end-to-end and EXPLAIN must still report pre_decode_filter: yes."

# CREATE ONLY returns the value object; field order is alphabetical.
[[test.results]]
value = "{ id: pfsparse:1, k00: 0, k01: 10, k02: 20, k03: 30, k04: 40, k05: 50, k06: 60, k07: 70, k08: 80, k09: 90, k10: 100, k11: 110, k12: 120, k13: 130, k14: 140, k15: 150 }"

[[test.results]]
value = "{ id: pfsparse:2, k00: 0, k01: 10, k02: 20, k03: 30, k04: 40, k05: 50, k06: 60, k07: 70, k08: 80, k09: 90, k10: 100, k11: 110, k12: 999, k13: 130, k14: 140, k15: 150 }"

# WHERE k03 = 30 AND k12 = 120 → only pfsparse:1.
[[test.results]]
value = "[{ id: pfsparse:1 }]"

# WHERE k03 = 30 AND k12 = 999 → only pfsparse:2.
[[test.results]]
value = "[{ id: pfsparse:2 }]"

# EXPLAIN: pre_decode_filter must report yes for the sparse-needle path.
[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: pfsparse, direction: Forward, predicate: k03 = 30 AND k12 = 120, pre_decode_filter: yes]
"'''

*/

CREATE ONLY pfsparse:1 SET k00 = 0, k01 = 10, k02 = 20, k03 = 30, k04 = 40, k05 = 50, k06 = 60, k07 = 70, k08 = 80, k09 = 90, k10 = 100, k11 = 110, k12 = 120, k13 = 130, k14 = 140, k15 = 150;
CREATE ONLY pfsparse:2 SET k00 = 0, k01 = 10, k02 = 20, k03 = 30, k04 = 40, k05 = 50, k06 = 60, k07 = 70, k08 = 80, k09 = 90, k10 = 100, k11 = 110, k12 = 999, k13 = 130, k14 = 140, k15 = 150;

SELECT id FROM pfsparse WHERE k03 = 30 AND k12 = 120;
SELECT id FROM pfsparse WHERE k03 = 30 AND k12 = 999;

EXPLAIN SELECT id FROM pfsparse WITH NOINDEX WHERE k03 = 30 AND k12 = 120;
