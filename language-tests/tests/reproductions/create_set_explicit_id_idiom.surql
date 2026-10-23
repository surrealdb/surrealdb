/**
[env]
namespace = true
database = true

[test]
reason = "A record id known before the data clause runs, named or from a range target, is read as `id` in SET on CREATE; a generated id is not known yet and `id` reads NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: planet:earth, name: 'earth' }]"

[[test.results]]
value = "[{ id: planet:earth, name: 'earth' }]"

[[test.results]]
value = "[{ id: person:1, name: 1 }, { id: person:2, name: 2 }, { id: person:3, name: 3 }]"

[[test.results]]
value = "[{ name: 1 }, { name: 2 }, { name: 3 }]"

[[test.results]]
error = "There was a problem running the id() function. no such method found for the none type"
*/

DEFINE TABLE planet SCHEMAFULL;
DEFINE FIELD name ON planet TYPE string;

CREATE planet:earth SET name = id.id();
SELECT * FROM planet:earth;

CREATE |person:1..=3| SET name = id.id();
SELECT name FROM person ORDER BY name;

CREATE planet SET name = id.id();
