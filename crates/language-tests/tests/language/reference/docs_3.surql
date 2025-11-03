/**
[test]
reason = "Example from https://surrealdb.com/docs/surrealql/datamodel/references on ON DELETE IGNORE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ friended_by: [], friends: [person:two], id: person:one }]"

[[test.results]]
value = "[{ friended_by: [person:one], id: person:two }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "{ friended_by: [], id: person:two }"

[env.capabilities]
allow-experimental = ["record_references"]

*/

-- Default, behaviour, so identical to:
-- DEFINE FIELD friends ON person TYPE option<array<record<person>>> REFERENCE;
DEFINE FIELD friends ON person TYPE option<array<record<person>>> REFERENCE ON DELETE IGNORE;
DEFINE FIELD friended_by ON person COMPUTED <~person;

CREATE person:one SET friends = [person:two];
CREATE person:two;
DELETE person:one;
person:two.*;