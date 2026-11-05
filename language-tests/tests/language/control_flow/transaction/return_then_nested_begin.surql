/**
[test]
reason = "A BEGIN after a RETURN in a block is a nested BEGIN, since the RETURN skips nothing (register #8): it aborts the block, the RETURN's slot reports the failed transaction, and the COMMIT names the nested BEGIN"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Tried to start a transaction while another transaction was open"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a nested BEGIN"

[[test.results]]
value = "2"

[env]
namespace = "test"
database = "test"

*/

BEGIN;
RETURN 1;
BEGIN;
COMMIT;
RETURN 2;
