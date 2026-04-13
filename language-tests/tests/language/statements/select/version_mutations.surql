/**
[env]
backend = ["mem", "rocksdb", "surrealkv"]
versioned = true
timeout = 5000

[test]
reason = "VERSION clause correctly shows historical state before UPDATE and DELETE mutations"

[[test.results]]
value = "[{ id: person:1, name: 'Alice' }]"

[[test.results]]
value = "[{ id: person:2, name: 'Bob' }]"

[[test.results]]
value = "[{ id: person:3, name: 'Charlie' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:1, name: 'Alice_v2' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: person:1, name: 'Alice_v2' }, { id: person:3, name: 'Charlie' }]"

[[test.results]]
value = "[{ id: person:1, name: 'Alice' }, { id: person:2, name: 'Bob' }, { id: person:3, name: 'Charlie' }]"

[[test.results]]
value = "[{ id: person:2, name: 'Bob' }]"

[[test.results]]
value = "[{ id: person:1, name: 'Alice' }]"

[[test.results]]
value = "[{ id: person:1, name: 'Alice_v2' }]"

*/

-- Create initial records
CREATE person:1 SET name = 'Alice';
CREATE person:2 SET name = 'Bob';
CREATE person:3 SET name = 'Charlie';

-- Capture timestamp after initial creation
SLEEP 50ms;
LET $ts = time::now();
SLEEP 50ms;

-- Mutate: update one record, delete another
UPDATE person:1 SET name = 'Alice_v2';
DELETE person:2;

-- Current state: only person:1 (updated) and person:3 remain
SELECT * FROM person;

-- VERSION $ts: all three original records with original values
SELECT * FROM person VERSION $ts;

-- VERSION $ts + WHERE: deleted record still visible
SELECT * FROM person WHERE name = 'Bob' VERSION $ts;

-- VERSION $ts on specific record: shows original value
SELECT * FROM person:1 VERSION $ts;

-- Current state of same record: shows updated value
SELECT * FROM person:1;
