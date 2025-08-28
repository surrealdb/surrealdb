/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: house:one, using: [] }]"

[[test.results]]
value = "[{ id: utility:gas }, { id: utility:water }]"

[[test.results]]
value = "[{ connected_to: [house:one], id: utility:gas }]"

[[test.results]]
value = "[{ connected_to: [house:one], id: utility:water }]"

[[test.results]]
value = "{ id: house:one, using: [utility:gas, utility:water] }"

[[test.results]]
error = "Cannot delete `house:one` as it is referenced by `utility:gas` with an ON DELETE REJECT clause"

[[test.results]]
value = "[{ connected_to: [], id: utility:gas }]"

[[test.results]]
value = "[{ connected_to: [], id: utility:water }]"

[[test.results]]
value = "[{ connected_to: [], id: utility:gas }, { connected_to: [], id: utility:water }]"

[[test.results]]
value = "[{ id: house:one, using: [] }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[env.capabilities]
allow-experimental = ["record_references"]

*/
DEFINE FIELD connected_to ON utility TYPE option<array<record<house>>> REFERENCE ON DELETE REJECT;
DEFINE FIELD using ON house COMPUTED <~utility;

CREATE house:one;
CREATE utility:gas, utility:water;

UPDATE utility:gas   SET connected_to = [house:one];
UPDATE utility:water SET connected_to = [house:one];
house:one.*;
DELETE house:one;

UPDATE utility:gas   SET connected_to -= house:one;
UPDATE utility:water SET connected_to -= house:one;
SELECT * FROM utility;
SELECT * FROM house;

DELETE house:one;

DELETE utility;

house:one.*;
