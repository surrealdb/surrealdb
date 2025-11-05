/**
[test]
reason = "Test transaction with RETURN statement"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:tobie }]"

[[test.results]]
value = "[{ id: person:jaime }]"

[[test.results]]
value = "{ tobie: person:tobie, jaime: person:jaime }"

[[test.results]]
value = "NONE"

[env]
namespace = "test"
database = "test"

*/

BEGIN;
CREATE person:tobie;
CREATE person:jaime;
RETURN { tobie: person:tobie, jaime: person:jaime };
COMMIT;

