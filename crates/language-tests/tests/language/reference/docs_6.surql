/**
[test]
reason = "Example from https://surrealdb.com/docs/surrealql/datamodel/references on ON DELETE REJECT"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: house:one, using: [] }]"

[[test.results]]
value = "[{ connected_to: [house:one], id: utility:gas }, { connected_to: [house:one], id: utility:water }]"

[[test.results]]
value = "{ id: house:one, using: [utility:gas, utility:water] }"

[[test.results]]
error = "Cannot delete `house:one` as it is referenced by `utility:gas` with an ON DELETE REJECT clause"

[[test.results]]
value = "[{ connected_to: [], id: utility:gas }]"

[[test.results]]
value = "[{ connected_to: [], id: utility:water }]"

[[test.results]]
value = "[{ id: house:one, using: [] }]"

[env.capabilities]
allow-experimental = ["record_references"]

*/ 

DEFINE FIELD connected_to ON utility TYPE option<array<record<house>>> REFERENCE ON DELETE REJECT;
DEFINE FIELD using ON house COMPUTED <~utility;

CREATE house:one;
CREATE utility:gas, utility:water SET connected_to = [house:one];

house:one.*;
DELETE house:one RETURN BEFORE;

UPDATE utility:gas   SET connected_to -= house:one;
UPDATE utility:water SET connected_to -= house:one;

DELETE house:one RETURN BEFORE;