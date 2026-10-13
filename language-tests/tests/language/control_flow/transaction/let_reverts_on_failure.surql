/**
[test]
reason = "A request LET inside a block is undone when a statement of the block fails (register #1): the block is cancelled, so a rebinding returns to its earlier value and a new name is unbound again"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "An error occurred: e"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "[0, NONE]"

[env]
namespace = "test"
database = "test"

*/

LET $x = 0;
BEGIN;
LET $x = 1;
LET $y = 2;
THROW "e";
COMMIT;
RETURN [$x, $y];
