/**
[test]
reason = "A request LET inside a block is undone by CANCEL (register #1): a rebinding returns to its earlier value and a new name is unbound again"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[0, NONE]"

[env]
namespace = "test"
database = "test"

*/

LET $a = 0;
BEGIN;
LET $a = 1;
LET $b = 2;
CANCEL;
RETURN [$a, $b];
