/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-eval-query = true

[test]
reason = """
	Every read of the clock answers one instant per statement. Two top-level
	statements of a request read two instants; the statements of a block, of the
	body of a `FOR` and of a function body each read an instant of their own, so
	a `SLEEP` between two statements of a list advances the clock; and every
	frame one statement runs in reads that statement's instant: a subquery, a
	closure it applies, an `eval::surql` query and an embedded script, which
	includes a script's `surrealdb.query()`. A script returns a datetime to the
	millisecond, so its reads are compared by their milliseconds, after a sleep
	long enough to separate two wall-clock reads.
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

# Two statements of a block, two instants.
[[test.results]]
value = "true"

# FOR body
[[test.results]]
value = "NONE"

# Every iteration of the loop wrote the instant of its own statement.
[[test.results]]
value = "true"

# DEFINE FUNCTION
[[test.results]]
value = "NONE"

# A function body starts after the statement that calls it, and a subquery, a
# closure, a nested query and a script agree with the read of the statement
# that runs them.
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
	RETURN $x < time::now()
};

FOR $i IN 0..3 {
	SLEEP 5ms;
	CREATE clock_for SET at = time::now();
};
RETURN array::len(array::distinct((SELECT VALUE at FROM clock_for))) = 3;

DEFINE FUNCTION fn::clock() { RETURN time::now() };
{
	LET $outer = time::now();
	SLEEP 20ms;
	LET $closure = || time::now();
	RETURN [
		$outer < fn::clock(),
		time::now() = $closure(),
		time::now() = (SELECT VALUE time::now() FROM [1])[0],
		time::now() = eval::surql('RETURN time::now()'),
		time::millis(time::now()) = time::millis(function() { return surrealdb.functions.time.now(); }),
		time::millis(time::now()) = time::millis(function() { return surrealdb.query('RETURN time::now()'); })
	]
};
