/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL split_part rejects field position zero"

[[test.results]]
error = "An error occurred: field position must not be zero"
*/

SELECT split_part('a,b', ',', 0);
