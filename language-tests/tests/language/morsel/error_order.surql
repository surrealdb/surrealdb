/**
[env]
namespace = true
database = true
# Extra timeout: every scan below reads 3000 records, which splits into one morsel per record when
# the suite forces the smallest morsels, and RocksDB does not finish that in the default timeout
# while the suite runs in parallel.
timeout = 30000

[test]
reason = "a scan whose rows fail raises the error of one of the failing rows, and a limit over a scan without ORDER BY returns the requested number of rows or raises such an error, whatever width the scan runs at"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = true
match = '''$error = /^Could not cast into `int` using input `'[xy]'`$/'''

[[test.results]]
error = true
match = '''$error = /^Could not cast into `int` using input `'[xy]'`$/'''

[[test.results]]
error = true
match = '''$error = /^Could not cast into `int` using input `'[xy]'`$/'''

[[test.results]]
match = '''$result = 600 || ($error = /^Could not cast into `int` using input `'[xy]'`$/)'''

[[test.results]]
match = '''$result = 800 || ($error = /^Could not cast into `int` using input `'[xy]'`$/)'''

[[test.results]]
match = '''$result = 5 || ($error = /^Could not cast into `int` using input `'[xy]'`$/)'''

[[test.results]]
match = '''$result = 5 || ($error = /^Could not cast into `int` using input `'[xy]'`$/)'''

[[test.results]]
match = '''$result = 30 || ($error = /^Could not cast into `int` using input `'[xy]'`$/)'''

[[test.results]]
match = '''$result = 80 || ($error = /^Could not cast into `int` using input `'[xy]'`$/)'''
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
