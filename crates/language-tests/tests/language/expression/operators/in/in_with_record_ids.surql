/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: group:0foo, person_id: person:0hgohtxrngq4onq8oak8 }]"

[[test.results]]
value = "[{ id: group:0foo }]"

[[test.results]]
value = "[{ id: group:0foo }]"


*/
DEFINE TABLE group SCHEMAFULL;
DEFINE FIELD person_id ON TABLE group TYPE record;

DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON TABLE person TYPE string;

let $person = (CREATE person SET name = "john", id = "0hgohtxrngq4onq8oak8")[0];

CREATE group SET person_id = $person.id, id = "0foo";

SELECT id from group where person_id = person:0hgohtxrngq4onq8oak8;
SELECT id from group where person_id IN [person:0hgohtxrngq4onq8oak8];
