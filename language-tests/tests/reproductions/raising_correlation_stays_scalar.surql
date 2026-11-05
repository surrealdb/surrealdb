/**
[env]
namespace = true
database = true

[test]
reason = "a join evaluates its key for every left row whatever the build side holds, so a correlation that can raise must keep the subquery scalar"

[[test.results]]
value = "[{ bad: 'x', id: o:1, k: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: r:1, k: 1 }]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"
*/

-- Evaluated as written, `k IN (SELECT VALUE k FROM r WHERE …)` evaluates the
-- subquery's condition once per right row per outer row: over an empty `r`
-- it evaluates it not at all. Lifted, `<int> $parent.bad` is the join's probe
-- key and runs for every left row, so the cast that the statement as written
-- never reaches would be reported. With a right row present both forms reach
-- it and both report.

CREATE o:1 SET k = 1, bad = 'x';
DEFINE TABLE r SCHEMALESS;

SELECT VALUE id FROM o WHERE k IN (SELECT VALUE k FROM r WHERE k = <int> $parent.bad);

CREATE r:1 SET k = 1;

SELECT VALUE id FROM o WHERE k IN (SELECT VALUE k FROM r WHERE k = <int> $parent.bad);
