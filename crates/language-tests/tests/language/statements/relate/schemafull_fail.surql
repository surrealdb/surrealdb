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
error = "Couldn't coerce value for field `reason` of `likes:2`: Expected `string` but found `true`"

[[test.results]]
error = "Couldn't coerce value for field `in` of `likes:3`: Expected `record<person>` but found `dog:1`"
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
