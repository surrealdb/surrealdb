/**
[env]
namespace = true
database = true

# Scans never split and no batch goes to a worker, whatever the run forces: the serial run's
# first failure and paging are what these statements pin. `error_order.surql` states what a scan
# that runs wide keeps.
[env.config]
morsel_split_rows = "18446744073709551615"

[test]
reason = "when a scan runs at width one the first failure in scan order is the failure raised, and a row past a spent limit or skipped by START is never judged"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'y'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "600"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "5"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "30"

[[test.results]]
value = "80"
*/

FOR $i IN 0..3000 { CREATE type::record('t', $i) SET n = $i, s = <string> $i };
UPDATE t:700 SET s = 'x' RETURN NONE;
UPDATE t:2200 SET s = 'y' RETURN NONE;
SELECT <int> s AS i FROM t;
SELECT <int> s AS i FROM t ORDER BY id DESC;
SELECT n, <int> s AS i FROM t WHERE n % 2 = 0;
array::len(SELECT n, <int> s AS i FROM t LIMIT 600);
array::len(SELECT n, <int> s AS i FROM t LIMIT 800);
array::len(SELECT n, <int> s AS i FROM t LIMIT 5 START 701);
array::len(SELECT n, <int> s AS i FROM t LIMIT 5 START 698);
array::len(SELECT n, <int> s AS i FROM t WHERE n % 100 = 7 OR n > 2450 LIMIT 30);
array::len(SELECT n, <int> s AS i FROM t WHERE n % 100 = 7 OR n > 2450 LIMIT 80);
