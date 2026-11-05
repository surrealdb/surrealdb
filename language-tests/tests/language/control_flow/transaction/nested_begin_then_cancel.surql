/**
[test]
reason = "A CANCEL after a nested BEGIN is skipped like the other statements of the aborted block"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Tried to start a transaction while another transaction was open"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
value = "1"

[env]
namespace = "test"
database = "test"

*/

BEGIN;
BEGIN;
BEGIN;
CANCEL;
RETURN 1;
