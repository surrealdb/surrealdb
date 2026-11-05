/**
[test]
reason = "A CANCEL after a RETURN in a block rewrites every recorded slot, the RETURN's included, to the cancelled transaction (registers #8 and #20), and nothing the block wrote survives"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The table 'g' does not exist"

[env]
namespace = "test"
database = "test"

*/

BEGIN;
CREATE g:1;
RETURN 1;
CREATE g:2;
CANCEL;
SELECT * FROM g;
