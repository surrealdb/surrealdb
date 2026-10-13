/**
[env]
namespace = true
database = true

[test]
reason = """
A non-deterministic projection is evaluated once per row, and ORDER BY on its
alias sorts by the value the row actually returns. So the output of
`SELECT rand() AS r FROM t ORDER BY r` is ascending in the `r` values it
reports; a second draw for the sort key would report values in an order that
contradicts the ORDER BY.

Each alias is its own draw: `SELECT rand() AS a, rand() AS b` gives a row two
independent values, and only the alias named by ORDER BY is ordered. This test
asserts the ordered alias, not the disorder of the unordered one, which is
random rather than guaranteed.
"""
issue = 472

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "12"

[[test.results]]
value = "12"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[12, 12]"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"
*/

{
	FOR $i IN 0..12 {
		CREATE type::record("n", $i) SET v = $i;
	};
	RETURN "OK";
};

-- One draw per row, and the projected value is the one that was sorted
LET $rows = SELECT rand() AS r FROM n ORDER BY r;
RETURN array::len($rows);
RETURN array::len(array::distinct($rows.r));
RETURN $rows.r == array::sort($rows.r);

LET $desc = SELECT rand() AS r FROM n ORDER BY r DESC;
RETURN $desc.r == array::sort::desc($desc.r);

-- Independent draws per alias; the ordered alias still agrees with its key
LET $two = SELECT rand() AS a, rand() AS b FROM n ORDER BY a;
RETURN [array::len(array::distinct($two.a)), array::len(array::distinct($two.b))];
RETURN $two.a == array::sort($two.a);

-- Same contract for a clock-backed expression
LET $stamped = SELECT time::now() AS t FROM n ORDER BY t;
RETURN $stamped.t == array::sort($stamped.t);
