/**
[test]
reason = "Test basic transaction with BEGIN/COMMIT"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:tobie }]"

[[test.results]]
value = "[{ id: person:jaime }]"

[[test.results]]
value = "NONE"

[env]
namespace = "test"
database = "test"

*/

BEGIN;
CREATE person:tobie;
CREATE person:jaime;
COMMIT;

