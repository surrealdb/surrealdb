/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-eval-query = true

[test]
reason = """
	Every read of the clock answers one instant per top-level statement (register
	row #7). Two top-level statements of a request read two instants, a `SLEEP`
	inside one statement does not advance its instant, and every frame the
	statement runs in reads the statement's own: a `FOR` body, a subquery, a
	function body, a closure, an `eval::surql` query and an embedded script,
	which includes a script's `surrealdb.query()`. A script returns a datetime
	to the millisecond, so its reads are compared by their milliseconds, after
	a sleep long enough to separate two wall-clock reads.
"""

# $a = time::now()
[[test.results]]
value = "NONE"

# SLEEP
[[test.results]]
value = "NONE"

# $b = time::now()
[[test.results]]
value = "NONE"

# Two statements, two instants.
[[test.results]]
value = "true"

# One statement holding a sleep reads one instant.
[[test.results]]
value = "true"

# FOR body
[[test.results]]
value = "NONE"

# Every iteration of one statement wrote the same instant.
[[test.results]]
value = "true"

# DEFINE FUNCTION
[[test.results]]
value = "NONE"

# A function body, a closure, a subquery, a nested query and a script agree
# with the read of the statement that runs them.
[[test.results]]
value = "[true, true, true, true, true, true]"
*/

LET $a = time::now();
SLEEP 20ms;
LET $b = time::now();
RETURN $a < $b;

{
	LET $x = time::now();
	SLEEP 20ms;
	RETURN $x = time::now()
};

FOR $i IN 0..3 {
	SLEEP 5ms;
	CREATE clock_for SET at = time::now();
};
RETURN array::len(array::distinct((SELECT VALUE at FROM clock_for))) = 1;

DEFINE FUNCTION fn::clock() { RETURN time::now() };
{
	LET $outer = time::now();
	SLEEP 20ms;
	LET $closure = || time::now();
	RETURN [
		$outer = fn::clock(),
		$outer = $closure(),
		$outer = (SELECT VALUE time::now() FROM [1])[0],
		$outer = eval::surql('RETURN time::now()'),
		time::millis($outer) = time::millis(function() { return surrealdb.functions.time.now(); }),
		time::millis($outer) = time::millis(function() { return surrealdb.query('RETURN time::now()'); })
	]
};
