/**
[env]
namespace = true
database = true
timeout = 60000

[test]
reason = "a top-k scan whose body may raise runs every entry, as the serial scan does, so a failure on a row the top-k would reject is raised, whatever width the scan runs at"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Could not cast into `int` using input `'y'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Cannot perform addition with '9223372036854775807' and '1'"

[[test.results]]
error = "Cannot perform multiplication with 'string' and 'int'"

[[test.results]]
value = "[{ k: 0, n: 35 }, { k: 0, n: 42 }, { k: 0, n: 49 }, { k: 0, n: 56 }, { k: 0, n: 63 }, { k: 0, n: 70 }, { k: 0, n: 77 }, { k: 0, n: 84 }, { k: 0, n: 91 }, { k: 0, n: 98 }]"

[[test.results]]
value = "[{ k: 6, n: 41 }, { k: 6, n: 48 }, { k: 6, n: 55 }, { k: 6, n: 62 }, { k: 6, n: 69 }, { k: 6, n: 76 }, { k: 6, n: 83 }, { k: 6, n: 90 }, { k: 6, n: 97 }, { k: 6, n: 104 }]"
*/

FOR $i IN 0..3000 { CREATE type::record('t', $i) SET n = $i, k = $i % 7, s = <string> $i, v = $i };
UPDATE t:700 SET s = 'x' RETURN NONE;
UPDATE t:2200 SET s = 'y', v = 'oops', big = 9223372036854775807 RETURN NONE;
SELECT n, k FROM t WHERE n = 700 OR <int> s >= 0 ORDER BY k LIMIT 3;
SELECT n, k FROM t WHERE <int> s >= 0 ORDER BY k DESC LIMIT 10 START 5;
SELECT n, k FROM t WHERE (big ?? 0) + 1 >= 0 ORDER BY k LIMIT 3;
SELECT n, k FROM t WHERE v * 2 >= 0 ORDER BY k LIMIT 3;
SELECT n, k FROM t ORDER BY k LIMIT 10 START 5;
SELECT n, k FROM t ORDER BY k DESC LIMIT 10 START 5;
