/**
[test]
reason = """
A LET inside a transaction that is cancelled is undone with it: `$a`, bound
inside a committed transaction, keeps its new value, and `$b`, bound inside a
cancelled one, reads the value it held before BEGIN.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "0"

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


