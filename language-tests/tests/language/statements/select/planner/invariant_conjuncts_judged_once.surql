/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
The conjuncts of a WHERE clause that read nothing of the row — parameters,
session values, deterministic builtins over them — and come before every
conjunct that may raise are judged once per run of the statement, at the first
row the filter judges (EXPLAIN shows them as `once`); the rest are judged per
row. A verdict that refuses admits no row, for a read and for the rows a write
reads. A conjunct that raises raises at the first row, and over no rows raises
nothing. A parameter conjunct written after a conjunct that may raise stays
per row. Each statement returns what the predicate judged per row returns.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: n]
    Filter [ctx: Db] [once: true = true AND 3 = 3, predicate: n > 15]
        TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = "[16, 17, 18, 19]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Could not cast into `int` using input `'abc'`"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: n]
    Filter [ctx: Db] [predicate: <int> n > 15 AND true = true]
        TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = "[16, 17, 18, 19]"

[[test.results]]
value = "[0, 1, 2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[1, 1, 1, NONE]"
*/

DEFINE TABLE t SCHEMALESS;
DEFINE TABLE empty SCHEMALESS;
FOR $i IN 0..20 { CREATE type::record('t', $i) SET n = $i };
LET $on = true;
LET $off = false;
LET $s = 'abc';

EXPLAIN SELECT VALUE n FROM t WHERE $on = true AND string::len($s) = 3 AND n > 15;
SELECT VALUE n FROM t WHERE $on = true AND string::len($s) = 3 AND n > 15;
SELECT VALUE n FROM t WHERE $off = true AND n > 15;
SELECT VALUE n FROM t WHERE <int> $s > 0 AND n > 15;
SELECT VALUE n FROM empty WHERE <int> $s > 0 AND n > 15;
EXPLAIN SELECT VALUE n FROM t WHERE <int> n > 15 AND $on = true;
SELECT VALUE n FROM t WHERE <int> n > 15 AND $on = true;
UPDATE t SET m = 1 WHERE $on = true AND n < 3 RETURN VALUE n;
UPDATE t SET m = 2 WHERE $off = true AND n < 3 RETURN VALUE n;
SELECT VALUE m FROM t WHERE n < 4;
