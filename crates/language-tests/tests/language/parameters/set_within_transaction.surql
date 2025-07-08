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

let $a = 0;
let $b = 0;

BEGIN;
let $a = 1;
COMMIT;

BEGIN;
let $b = 2;
CANCEL;

$a;
$b;


