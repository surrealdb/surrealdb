/**
[test]
reason = """
A cancelled transaction undoes both its DEFINE PARAM and its LET: `$b` holds
nothing again, and `$a` names no parameter.
"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/

BEGIN;
DEFINE PARAM $a VALUE 1;
let $b = $a;
CANCEL;

$b;
$a;
