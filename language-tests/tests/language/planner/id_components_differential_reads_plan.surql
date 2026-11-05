/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	The plans behind id_components_differential_reads: the comparisons there
	are worth something only if the default side reads a key range where the
	rule allows one, and a table scan where it must not. An equality prefix
	and a datetime range read one key range; an IN on the int reads one per
	element, an integral float or decimal element counting as the int it
	equals; a fractional int bound rounds inwards; an int bound outside i64,
	NaN, a fractional equality and a NONE parameter leave the key at the
	equalities before them; a string compared with an int, inequalities and
	an index into the id that names no component read the whole table. A
	descending order over the components after a pinned one reads backwards,
	and NUMERIC or an alias that shadows a component claims no order.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = '''string::contains($result, "KeyRangeScan [ctx: Db] [table: k, key: s = 'a', i = 2, range: t > d'2024-01-01T00:00:00Z', direction: Forward, granularity: second, residual: yes]")'''
error = false

[[test.results]]
match = '''string::contains($result, "KeyRangeScan [ctx: Db] [table: k, key: s = 'ab', i IN [0, 7, 2], range: t <= d'2024-01-01T00:00:00.999999999Z', ranges: 3, direction: Forward, granularity: second, residual: yes]")'''
error = false

[[test.results]]
match = '''string::contains($result, "KeyRangeScan [ctx: Db] [table: k, key: s = 'B', range: i >= 3, direction: Forward, residual: yes]")'''
error = false

[[test.results]]
match = '''string::contains($result, "KeyRangeScan [ctx: Db] [table: k, key: s = 'B', direction: Forward, residual: yes]")'''
error = false

[[test.results]]
match = '''string::contains($result, "KeyRangeScan [ctx: Db] [table: k, key: s = 'B', direction: Forward, residual: yes]")'''
error = false

[[test.results]]
match = '''string::contains($result, "KeyRangeScan [ctx: Db] [table: k, key: s = 'B', direction: Forward, residual: yes]")'''
error = false

[[test.results]]
match = '''string::contains($result, "TableScan [ctx: Db] [table: k, direction: Forward]") AND !string::contains($result, "KeyRangeScan")'''
error = false

[[test.results]]
match = '''string::contains($result, "KeyRangeScan [ctx: Db] [table: k, key: s = 'a', direction: Forward, residual: yes]")'''
error = false

[[test.results]]
match = '''string::contains($result, "TableScan [ctx: Db] [table: k, direction: Forward]") AND !string::contains($result, "KeyRangeScan")'''
error = false

[[test.results]]
match = '''string::contains($result, "KeyRangeScan [ctx: Db] [table: k, key: s = 'a', direction: Backward, ordered: second, residual: yes]") AND string::contains($result, "Sort [ctx: Db] [sort_keys: i DESC, t DESC, keep: 4]")'''
error = false

[[test.results]]
match = '''string::contains($result, "KeyRangeScan [ctx: Db] [table: k, key: s = 'a', direction: Forward, residual: yes]") AND string::contains($result, "sort_keys: i DESC NUMERIC, t ASC")'''
error = false

[[test.results]]
match = '''string::contains($result, "KeyRangeScan [ctx: Db] [table: k, key: s = 'a', direction: Forward, residual: yes]") AND string::contains($result, "sort_keys: n DESC")'''
error = false

[[test.results]]
match = '''string::contains($result, "TableScan [ctx: Db] [table: k, direction: Forward]") AND !string::contains($result, "KeyRangeScan")'''
error = false
*/

DEFINE TABLE k SCHEMALESS;
DEFINE FIELD id ON k TYPE [s: string, i: int, t: datetime];
LET $none = NONE;
EXPLAIN SELECT s, i, t, n FROM k WHERE s = 'a' AND i = 2 AND t > d'2024-01-01T00:00:00Z' ORDER BY t, n;
EXPLAIN SELECT s, i, t, n FROM k WHERE s = 'ab' AND i IN [0, 7, 2.0, 7dec] AND t <= d'2024-01-01T00:00:00.999999999Z' ORDER BY i DESC, t DESC;
EXPLAIN SELECT VALUE n FROM k WHERE s = 'B' AND i > 2.5 ORDER BY n;
EXPLAIN SELECT VALUE n FROM k WHERE s = 'B' AND i >= 9.3e18 ORDER BY n;
EXPLAIN SELECT VALUE n FROM k WHERE s = 'B' AND i > NaN ORDER BY n;
EXPLAIN SELECT VALUE n FROM k WHERE s = 'B' AND i = 2.5 ORDER BY n;
EXPLAIN SELECT VALUE n FROM k WHERE s = 1 ORDER BY n;
EXPLAIN SELECT VALUE n FROM k WHERE s = 'a' AND t > $none ORDER BY n;
EXPLAIN SELECT VALUE n FROM k WHERE s != 'a' AND i NOT IN [0, 2] AND v IS NONE ORDER BY n;
EXPLAIN SELECT VALUE n FROM k WHERE s = 'a' ORDER BY i DESC, t DESC LIMIT 3 START 1;
EXPLAIN SELECT VALUE [s, i, t] FROM k WHERE s = 'a' ORDER BY i NUMERIC DESC, t LIMIT 4;
EXPLAIN SELECT n AS i, s FROM k WHERE s = 'a' ORDER BY i DESC LIMIT 3;
EXPLAIN SELECT VALUE n FROM k WHERE id[-1] = d'2024-01-01T00:00:00Z' ORDER BY n;
