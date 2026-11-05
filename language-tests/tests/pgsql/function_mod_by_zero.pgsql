/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL raises division by zero for mod by zero, and a logarithm error for a value that is not positive"

[[test.results]]
error = "An error occurred: division by zero"
*/

SELECT mod(1, 0);
