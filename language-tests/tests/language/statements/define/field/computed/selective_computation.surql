/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Tests that computed fields are evaluated correctly with selective computation and topological ordering"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ age: 25, can_drive: true, display: 'Alice', id: person:a, name: 'Alice' }]"

[[test.results]]
value = "[{ age: 16, can_drive: false, display: 'Bob', id: person:b, name: 'Bob' }]"

[[test.results]]
value = "[{ age: 25, can_drive: true, display: 'Alice', id: person:a, name: 'Alice' }, { age: 16, can_drive: false, display: 'Bob', id: person:b, name: 'Bob' }]"

[[test.results]]
value = "[{ can_drive: true, id: person:a }, { can_drive: false, id: person:b }]"

[[test.results]]
value = "[{ display: 'Alice', id: person:a }, { display: 'Bob', id: person:b }]"

[[test.results]]
value = "[{ can_drive: true, display: 'Alice', id: person:a }, { can_drive: false, display: 'Bob', id: person:b }]"

[[test.results]]
value = "[{ age: 25, id: person:a, name: 'Alice' }, { age: 16, id: person:b, name: 'Bob' }]"

*/

-- Set up a table with computed fields that reference stored fields
DEFINE TABLE person SCHEMALESS;
DEFINE FIELD can_drive ON person COMPUTED age >= 18;
DEFINE FIELD display ON person COMPUTED name;

-- Create records
CREATE person:a SET name = 'Alice', age = 25;
CREATE person:b SET name = 'Bob', age = 16;

-- SELECT * should compute all fields
SELECT * FROM person ORDER BY id;

-- SELECT specific computed fields with id for ordering
SELECT id, can_drive FROM person ORDER BY id;

-- SELECT display with id
SELECT id, display FROM person ORDER BY id;

-- SELECT both computed fields
SELECT id, can_drive, display FROM person ORDER BY id;

-- SELECT only stored fields (no computed fields needed)
SELECT id, name, age FROM person ORDER BY id;
