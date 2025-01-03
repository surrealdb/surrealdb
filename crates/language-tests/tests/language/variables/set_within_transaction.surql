/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
value = "1"

[[test.results]]
value = "2"

*/

BEGIN;
$a = 1;
COMMIT;

BEGIN;
$b = 2;
CANCEL;

$a;
$b;


