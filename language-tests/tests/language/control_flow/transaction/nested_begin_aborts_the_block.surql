/**
[test]
reason = "A BEGIN inside an open block aborts the block and every later statement reports why"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Tried to start a transaction while another transaction was open"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a nested BEGIN"

[[test.results]]
error = "The table 't' does not exist"

[env]
namespace = "test"
database = "test"

*/

BEGIN;
CREATE t:1;
BEGIN;
CREATE t:2;
COMMIT;
SELECT * FROM t;
