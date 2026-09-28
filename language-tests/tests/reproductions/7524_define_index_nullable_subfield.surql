/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "DEFINE INDEX on subfield of null | { ... } field on SCHEMAFULL table should succeed"
issue = 7524

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/

DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD address ON person TYPE null | { city: string } DEFAULT NULL;
DEFINE INDEX person_city ON person FIELDS address.city;
