/**
[test]
reason = "A USE inside a block is not undone by CANCEL (registers #1 and #11): only request LET bindings revert, the session's namespace and database stay as the USE left them"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'moved'"

[env]
namespace = "test"
database = "test"

*/

DEFINE DATABASE moved;
BEGIN;
USE DB moved;
CANCEL;
RETURN session::db();
