/**
[test]
reason = "Example from https://surrealdb.com/docs/surrealql/datamodel/references on ON DELETE UNSET"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:one }]"

[[test.results]]
value = "[{ comments: [comment:one], id: person:one }]"

[[test.results]]
value = "[{ author: [], id: comment:two, text: \"I don't get the joke here?\" }]"

[[test.results]]
value = "[{ comments: [comment:one, comment:two], id: person:one }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ author: [person:one], id: comment:one, text: 'Estonia is bigger than I expected!' }]"

*/

DEFINE FIELD comments ON person TYPE option<array<record<comment>>> REFERENCE ON DELETE UNSET;
DEFINE FIELD author ON comment COMPUTED <~person;

CREATE person:one;
UPDATE person:one SET comments += (CREATE ONLY comment:one SET text = "Estonia is bigger than I expected!").id;
-- Give this one a parameter name so it can be deleted later
CREATE comment:two SET text = "I don't get the joke here?";
UPDATE person:one SET comments += comment:two;
-- Now delete it
DELETE comment:two;
-- Only one comment shows up for person:one now
person:one.comments.*.*;