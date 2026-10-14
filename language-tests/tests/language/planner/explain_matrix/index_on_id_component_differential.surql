/**
[env]
namespace = true
database = true
# Extra timeout: the test runs a few thousand nested queries.
timeout = 30000

[env.capabilities]
allow-eval-query = true

[test]
reason = """
A b-tree index over a datetime component of the record id, defined on this
release, holds each entry as a chronological key, so a range over it seeks its
bounds as written and the predicate is judged again over every record it
reaches. Rows of
three feeds sit at whole seconds, a nanosecond past them, at every fraction
from a millisecond to the last nanosecond of a second, and a nanosecond before
the next one. Every bound among those instants, on either side and inclusive or
exclusive, through the index over the component, a compound index pinned on the
feed, and a compound index led by the component, returns the rows `WITH NOINDEX`
returns; so does the planner's own choice, its count, and its first rows in
`ORDER BY at, v`, as do two-sided ranges. The count pins how many of those reads
the index serves, so the comparison is not vacuous, and the COUNT index over a
component counts what a scan counts.
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
value = "NONE"

[[test.results]]
value = "[{ differ: 0, indexed: 210, total: 406 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"
*/
DEFINE TABLE tick SCHEMAFULL;
DEFINE FIELD id ON tick TYPE [feed: string, at: datetime];
DEFINE FIELD v ON tick TYPE int;
DEFINE INDEX by_at ON tick FIELDS at;
DEFINE INDEX by_feed_at ON tick FIELDS feed, at;
DEFINE INDEX by_at_v ON tick FIELDS at, v;
DEFINE INDEX f_count ON tick COUNT WHERE feed = 'f';
LET $base = d'2024-01-01T00:00:00Z';
LET $offs = [0ns, 1ns, 1ms, 499ms, 500ms, 999ms, 999999999ns, 1s, 1s1ns, 1s500ms, 2s, 2s999ms, 3s, 59s999ms, 1m];
FOR $i IN 0..90 {
	CREATE tick SET feed = ['f', 'g', 'h'][$i % 3], at = $base + $offs[$i % 15] + <duration> string::concat(<string> (($i - $i % 15) / 15 * 61), 's'), v = $i RETURN NONE;
};
LET $bounds = ["d'2023-12-31T23:59:59.999999999Z'", "d'2024-01-01T00:00:00Z'", "d'2024-01-01T00:00:00.000000001Z'", "d'2024-01-01T00:00:00.5Z'",
	"d'2024-01-01T00:00:00.999Z'", "d'2024-01-01T00:00:01Z'", "d'2024-01-01T00:00:01.5Z'", "d'2024-01-01T00:00:02Z'", "d'2024-01-01T00:00:02.999Z'",
	"d'2024-01-01T00:00:03.000000001Z'", "d'2024-01-01T00:01:00Z'", "d'2024-01-01T00:01:02.5Z'", "d'2025-01-01T00:00:00Z'", "d'0000-01-01T00:00:00Z'"];
FOR $op IN ['<', '<=', '>', '>=', '='] { FOR $b IN $bounds {
	FOR $q IN [
		[string::concat('at ', $op, ' ', $b), 'by_at'],
		[string::concat("feed = 'g' AND at ", $op, ' ', $b), 'by_feed_at'],
		[string::concat('at ', $op, ' ', $b, ' AND v > 10'), 'by_at_v']
	] {
		LET $a = array::sort(eval::surql(string::concat('SELECT VALUE v FROM tick WITH INDEX ', $q[1], ' WHERE ', $q[0])));
		LET $p = array::sort(eval::surql(string::concat('SELECT VALUE v FROM tick WHERE ', $q[0])));
		LET $c = eval::surql(string::concat('SELECT VALUE count() FROM tick WHERE ', $q[0], ' GROUP ALL'));
		LET $n = array::sort(eval::surql(string::concat('SELECT VALUE v FROM tick WITH NOINDEX WHERE ', $q[0])));
		LET $o = eval::surql(string::concat('SELECT VALUE v FROM tick WHERE ', $q[0], ' ORDER BY at, v LIMIT 5'));
		LET $on = eval::surql(string::concat('SELECT VALUE v FROM tick WITH NOINDEX WHERE ', $q[0], ' ORDER BY at, v LIMIT 5'));
		LET $e = <string> eval::surql(string::concat('EXPLAIN SELECT VALUE v FROM tick WITH INDEX ', $q[1], ' WHERE ', $q[0]));
		CREATE result SET q = $q[0], same = $a == $n AND $p == $n AND ($c[0] ?? 0) == array::len($n) AND $o == $on, indexed = string::contains($e, 'IndexScan'), a = $a, n = $n, c = $c, o = $o, on = $on RETURN NONE;
	};
}};
FOR $b1 IN $bounds { FOR $b2 IN $bounds {
	LET $w = string::concat('at > ', $b1, ' AND at <= ', $b2);
	LET $a = array::sort(eval::surql(string::concat('SELECT VALUE v FROM tick WITH INDEX by_at WHERE ', $w)));
	LET $n = array::sort(eval::surql(string::concat('SELECT VALUE v FROM tick WITH NOINDEX WHERE ', $w)));
	CREATE result SET q = $w, same = $a == $n, indexed = NONE, a = $a, n = $n RETURN NONE;
}};
SELECT count() AS total, count(indexed = true) AS indexed, count(!same) AS differ FROM result GROUP ALL;
SELECT VALUE q FROM result WHERE !same;
RETURN (SELECT count() FROM tick WHERE feed = 'f' GROUP ALL) == (SELECT count() FROM tick WITH NOINDEX WHERE feed = 'f' GROUP ALL);
