/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A statement cancelled by its TIMEOUT while a nested stored-function call is
suspended does not leave unresolved work behind: the statements after it in
the same session run normally.
"""
issue = 7302

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed because it exceeded the timeout: 20ms"

[[test.results]]
value = "2"

[[test.results]]
value = "[2]"
*/
DEFINE FUNCTION fn::slow() { SLEEP 150ms; RETURN 1; };
DEFINE FUNCTION fn::outer() { RETURN fn::slow() + 1; };
SELECT VALUE fn::outer() FROM [1, 2, 3] TIMEOUT 20ms;
RETURN fn::outer();
SELECT VALUE fn::outer() FROM [1] TIMEOUT 2s;
