/**
[env]
backend = ["mem", "rocksdb", "surrealkv"]
versioned = true
timeout = 5000

[test]
reason = "VERSION should resolve linked record fields using the schema at that point in time"

[[test.results]]
value = "[{ id: city:1, name: 'Paris' }]"

[[test.results]]
value = "[{ city: city:1, id: person:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: city:1, name: 'London' }]"

[[test.results]]
value = "[{ city: { name: 'London' } }]"

[[test.results]]
value = "[{ city: { name: 'Paris' } }]"

*/

-- When auto-dereferencing a RecordId field (e.g. person.address.city),
-- the linked record's fields should be read at the versioned timestamp,
-- and the schema used for the linked table should also be from that version.

CREATE city:1 SET name = 'Paris';
CREATE person:1 SET city = city:1;

SLEEP 50ms;
LET $t1 = time::now();
SLEEP 50ms;

-- Update the linked record
UPDATE city:1 SET name = 'London';

-- HEAD: dereference sees London
SELECT city.name FROM person:1;

-- VERSION $t1: dereference sees Paris
SELECT city.name FROM person:1 VERSION $t1;
