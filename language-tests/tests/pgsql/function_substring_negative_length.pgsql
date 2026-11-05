/**
[env]
namespace = true
database = true

[test]
reason = "A negative substring length is an error in PostgreSQL, where MySQL answers the empty string"

[[test.results]]
error = "An error occurred: negative substring length not allowed"
*/

SELECT substring('Hello', 2, -1) AS v;
