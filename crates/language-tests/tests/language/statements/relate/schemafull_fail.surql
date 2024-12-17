/**
[test]
reason = "schema compliance with relation statement"

[[test.results]]
value = "[{ id: person:1 }, { id: person:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: likes:1, in: person:1, out: person:2, reason: 'nice smile' }]"

[[test.results]]
error = "Found true for field `reason`, with record `likes:2`, but expected a string"

[[test.results]]
error = "Found dog:1 for field `in`, with record `likes:3`, but expected a record<person>"
*/

INSERT INTO person [
    { id: 1 },
    { id: 2 }
];

DEFINE TABLE likes TYPE RELATION FROM person TO person;
DEFINE FIELD reason ON likes TYPE string;

RELATE person:1 -> likes -> person:2 CONTENT {id: 1, reason: "nice smile"};
RELATE person:2 -> likes -> person:1 CONTENT {id: 2, reason: true};
RELATE dog:1 -> likes -> person:2 CONTENT {id: 3, reason: "nice smell"};
