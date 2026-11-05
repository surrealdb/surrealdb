/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A statement in a later transaction sees every definition committed before
	that transaction opened: those of the request's own earlier statements.
	Here a global parameter is defined, altered, overwritten and removed, and
	both a direct read and a read from inside a function body see the
	definition in force when the statement opens.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "5"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[6, 6]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[7, 7]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[NONE, NONE]"
*/

DEFINE PARAM $p VALUE 5;
RETURN $p;
DEFINE FUNCTION fn::p() { RETURN $p };

ALTER PARAM $p VALUE 6;
RETURN [$p, fn::p()];

DEFINE PARAM OVERWRITE $p VALUE 7;
RETURN [$p, fn::p()];

REMOVE PARAM $p;
RETURN [$p, fn::p()];
