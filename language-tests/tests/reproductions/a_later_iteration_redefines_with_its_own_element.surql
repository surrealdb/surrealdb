/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A schema statement in a loop body runs on every iteration, with that
	iteration's element. An `OVERWRITE` whose clause reads the element
	writes a new definition each time, so the loop leaves the last element's;
	only a statement guarded by the existence of what it names (`IF NOT
	EXISTS`) does nothing after the first iteration.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "4"

[[test.results]]
value = "NONE"

[[test.results]]
value = "4"

[[test.results]]
value = "NONE"

[[test.results]]
value = "3"
*/

FOR $i IN [3, 4] { DEFINE PARAM OVERWRITE $a VALUE $i };
$a;
FOR $i IN [3, 4] { DEFINE PARAM OVERWRITE $b VALUE $i; DEFINE PARAM OVERWRITE $b VALUE $i };
$b;
FOR $i IN [3, 4] { DEFINE PARAM IF NOT EXISTS $c VALUE $i };
$c;
