/**
[env]
namespace = true
database = true

[test]
reason = "Register #28, row IndexColumn: a builtin that draws randomness is not allowed in the column of an index, which must give the same value on every evaluation. The request is refused at plan time, before any statement of it runs."

[test.results]
request-error = "Invalid statement: a non-deterministic expression is not allowed in an index definition (statement 1)"
*/
DEFINE INDEX i ON q FIELDS rand();
