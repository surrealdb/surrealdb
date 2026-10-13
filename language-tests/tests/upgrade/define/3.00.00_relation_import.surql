/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:alice }]"

[[test.results]]
value = "[{ id: person:bob }]"

[[test.results]]
value = "[{ id: person:carol }]"

[[test.results]]
value = "[{ id: knows:⟨k1⟩, in: person:alice, out: person:bob, since: d'2025-01-01T00:00:00Z' }]"

[[test.results]]
value = "[{ id: knows:⟨k2⟩, in: person:bob, out: person:carol, since: d'2025-02-01T00:00:00Z' }]"

*/
DEFINE TABLE person SCHEMALESS;
DEFINE TABLE knows TYPE RELATION FROM person TO person SCHEMALESS;
CREATE person:alice;
CREATE person:bob;
CREATE person:carol;
RELATE person:alice -> knows:k1 -> person:bob SET since = d'2025-01-01T00:00:00Z';
RELATE person:bob -> knows:k2 -> person:carol SET since = d'2025-02-01T00:00:00Z';
