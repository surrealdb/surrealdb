/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL cannot take the logarithm of a negative number"

[[test.results]]
error = "An error occurred: cannot take logarithm of a negative number"
*/

SELECT log(-1);
