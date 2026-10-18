/**
[env]
namespace = true
database = true
# Every statement below writes hundreds of records, which the suite's forced configuration cuts into
# a batch for each record, and RocksDB does not finish that in the default timeout while the suite
# runs in parallel.
timeout = 30000

[test]
reason = "an UPDATE over every record of a table leaves the records, returns the rows and counts what it changed as the serial run does, whatever width its compute half runs at"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 900 }]"

[[test.results]]
value = "['n0 hi!', 'n1 hi!', 'n2 hi!', 'n3 hi!', 'n4 hi!']"

[[test.results]]
value = "['n895 hi!', 'n896 hi!', 'n897 hi!', 'n898 hi!', 'n899 hi!']"

[[test.results]]
value = "[0, 1, 2, 3, 4, 5]"

[[test.results]]
value = "[{ id: t:0, n: 0, name: 'n0 hi!', s: 'z' }, { id: t:1, n: 1, name: 'n1 hi!', s: 'z' }, { id: t:2, n: 2, name: 'n2 hi!', s: 'z' }, { id: t:3, n: 3, name: 'n3 hi!', s: 'z' }]"

[[test.results]]
value = '''[[{ op: 'change', path: '/s', value: '@@ -1,1 +1,1 @@\n-a\n+b\n' }], [{ op: 'change', path: '/s', value: '@@ -1,1 +1,1 @@\n-a\n+b\n' }], [{ op: 'change', path: '/s', value: '@@ -1,1 +1,1 @@\n-a\n+b\n' }]]'''

[[test.results]]
value = "[{ id: t:0, n: 0, name: 'n0 hi!', s: 'c' }, { id: t:1, n: 1, name: 'n1 hi!', s: 'c' }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ total: 404550 }]"

*/

DEFINE TABLE t;
FOR $i IN 0..900 { CREATE type::record('t', $i) SET n = $i, name = 'n' + <string> $i, s = <string> $i };
UPDATE t SET name = name + ' hi!' RETURN NONE;
SELECT count() FROM t WHERE string::ends_with(name, ' hi!') GROUP ALL;
SELECT VALUE name FROM t:0..5;
SELECT VALUE name FROM t:895..900;
UPDATE t SET s = 'z' WHERE n < 6 RETURN VALUE n;
UPDATE t SET s = 'a' WHERE n < 4 RETURN BEFORE;
UPDATE t SET s = 'b' WHERE n < 3 RETURN DIFF;
UPDATE t SET s = 'c' WHERE n < 2 RETURN AFTER;
SELECT count() FROM t WHERE s = 'z' GROUP ALL;
SELECT math::sum(n) AS total FROM t GROUP ALL;
