/**
[test]

[[test.results]]
value = 'NONE'

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

*/

DEFINE DB test;

BEGIN;
DEFINE PARAM $a VALUE 1;
let $b = $a;
CANCEL;

$b;
$a;
