/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL cannot take the logarithm of zero"

[[test.results]]
error = "An error occurred: cannot take logarithm of zero"
*/

SELECT ln(0);
