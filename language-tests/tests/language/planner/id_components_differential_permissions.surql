/**
[env]
imports = ["language/planner/id_components_differential_permissions_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:bob" }

[test]
reason = """
	Differential reads by a record user whose table permission reads the
	id components. Every statement returns true when the default plan over
	the keyed table `k` (which may read a key range) returns what the same
	query over `u`, its unkeyed twin under the same permission, returns:
	equality prefixes and datetime ranges, a component the permission
	refuses, parameters, orders under LIMIT, counts, and a block that updates
	by the same predicate.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"
*/

LET $from = d'2024-01-01T00:00:00.5Z';
RETURN count(SELECT * FROM k) == count(SELECT * FROM u) AND count(SELECT * FROM k) > 0 AND count(SELECT * FROM k) < 60;
RETURN (SELECT s, t, n FROM k WHERE s = 'a' ORDER BY t, n) == (SELECT s, t, n FROM u WHERE s = 'a' ORDER BY t, n);
RETURN (SELECT s, t, n FROM k WHERE s = 'c' ORDER BY t, n) == (SELECT s, t, n FROM u WHERE s = 'c' ORDER BY t, n);
RETURN (SELECT s, t, n FROM k WHERE s IN ['a', 'c'] AND t >= $from AND t < d'2024-01-01T00:00:01.000000001Z' ORDER BY n) == (SELECT s, t, n FROM u WHERE s IN ['a', 'c'] AND t >= $from AND t < d'2024-01-01T00:00:01.000000001Z' ORDER BY n);
RETURN (SELECT s, t, n FROM k WHERE s = 'b' AND t > d'2024-01-01T00:00:00.999999999Z' ORDER BY n) == (SELECT s, t, n FROM u WHERE s = 'b' AND t > d'2024-01-01T00:00:00.999999999Z' ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE s = 'a' ORDER BY t DESC, n DESC LIMIT 3) == (SELECT VALUE n FROM u WHERE s = 'a' ORDER BY t DESC, n DESC LIMIT 3);
RETURN (SELECT VALUE n FROM k WHERE s > 'a' ORDER BY s, t, n LIMIT 4 START 2) == (SELECT VALUE n FROM u WHERE s > 'a' ORDER BY s, t, n LIMIT 4 START 2);
RETURN (SELECT VALUE n FROM k WHERE t = d'2024-01-01T00:00:01Z' ORDER BY n) == (SELECT VALUE n FROM u WHERE t = d'2024-01-01T00:00:01Z' ORDER BY n);
RETURN (SELECT s, count() AS c FROM k WHERE t < d'2024-01-01T00:00:02Z' GROUP BY s ORDER BY s) == (SELECT s, count() AS c FROM u WHERE t < d'2024-01-01T00:00:02Z' GROUP BY s ORDER BY s);
RETURN {
	LET $a = array::sort((SELECT VALUE n FROM u WHERE s = 'b' AND t >= $from));
	LET $b = array::sort((UPDATE k SET hit = true WHERE s = 'b' AND t >= $from RETURN VALUE n));
	$a == $b AND array::len($a) > 0
};
