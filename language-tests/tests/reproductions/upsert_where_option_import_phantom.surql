/**
[env]
namespace = true
database = true

[test]
reason = "UPSERT ... WHERE under OPTION IMPORT must not create phantom records via the guaranteed-record mechanism"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ active: true, id: person:1, name: 'a' }]"

*/

OPTION IMPORT;
CREATE person:1 SET active = true;
UPSERT person SET name = 'a' WHERE active = true;
SELECT * FROM person;
