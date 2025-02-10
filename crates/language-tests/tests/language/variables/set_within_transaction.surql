/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
value = "1"

[[test.results]]
value = "2"

*/

$a = 0;
$b = 0;

BEGIN;
$a = 1;
COMMIT;

BEGIN;
$b = 2;
CANCEL;

$a;
$b;


