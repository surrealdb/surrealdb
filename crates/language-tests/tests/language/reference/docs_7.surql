/**
[test]
reason = "Example from https://surrealdb.com/docs/surrealql/datamodel/references on ON DELETE THEN"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ comments: [comment:one], id: person:one }]"

[[test.results]]
value = "{ author: [], id: comment:two, text: \"I don't get the joke here?\" }"

[[test.results]]
value = "[{ comments: [comment:one, comment:two], id: person:one }]"

[[test.results]]
value = "[{ author: [], id: comment:two, text: \"I don't get the joke here?\" }]"

[[test.results]]
value = "[{ comments: [comment:one, comment:two], id: person:one }]"

[env.capabilities]
allow-experimental = ["record_references"]

*/

DEFINE FIELD comments ON person TYPE option<array<record<comment>>> REFERENCE ON DELETE THEN {
    UPDATE $this SET
        deleted_comments += $reference.id,
        comments -= $reference.id;
};
DEFINE FIELD author ON comment COMPUTED <~person;

CREATE ONLY comment:one SET text = "Estonia is bigger than I expected!";
CREATE person:one SET comments += comment:one;
CREATE ONLY comment:two SET text = "I don't get the joke here?";
UPDATE person:one SET comments += comment:two;
DELETE comment:two RETURN BEFORE;
SELECT * FROM person:one;