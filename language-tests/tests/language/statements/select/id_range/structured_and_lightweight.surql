/**
[env]
namespace = true
database = true

[test]
reason = """
	A table whose record id names its components reads a predicate on them as key ranges, and a
	predicate on `id` itself is a comparison of whole record ids that no key range covers: both
	answer as a scan of the whole table does. A relation table stores no record range, and a
	predicate on its `id` answers the same.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ city: 'a', id: reading:['a', 1], time: 1, v: 1 }]"

[[test.results]]
value = "[{ city: 'a', id: reading:['a', 2], time: 2, v: 2 }]"

[[test.results]]
value = "[{ city: 'b', id: reading:['b', 1], time: 1, v: 3 }]"

[[test.results]]
value = "[{ city: 'b', id: reading:['b', 3], time: 3, v: 4 }]"

[[test.results]]
value = "[3, 4]"

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[2, 1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[2, 1]"

[[test.results]]
value = "[1, 3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[edge:[person:2, person:3], edge:[person:3, person:1]]"

[[test.results]]
value = "[edge:[person:1, person:2], edge:[person:2, person:3]]"

[[test.results]]
value = "[edge:[person:1, person:2]]"

[[test.results]]
value = "[edge:[person:1, person:2], edge:[person:2, person:3], edge:[person:3, person:1]]"

[[test.results]]
value = "[]"
*/

DEFINE TABLE reading SCHEMALESS;
DEFINE FIELD id ON reading TYPE [city: string, time: int];
CREATE reading:['a', 1] SET v = 1;
CREATE reading:['a', 2] SET v = 2;
CREATE reading:['b', 1] SET v = 3;
CREATE reading:['b', 3] SET v = 4;
SELECT VALUE v FROM reading WHERE id > reading:['a', 2];
SELECT VALUE v FROM reading WHERE id >= reading:['a', 2] AND id < reading:['b', 3];
SELECT VALUE v FROM reading WHERE city = 'a' AND time > 1;
SELECT VALUE v FROM reading WHERE id < reading:['b', 1] ORDER BY id DESC;
CREATE person:1, person:2, person:3 RETURN NONE;
RELATE person:1->knows:1->person:2 SET w = 1 RETURN NONE;
RELATE person:2->knows:2->person:3 SET w = 2 RETURN NONE;
RELATE person:1->knows:3->person:3 SET w = 3 RETURN NONE;
SELECT VALUE w FROM knows WHERE id > knows:1;
SELECT VALUE w FROM knows WHERE id >= knows:1 AND id <= knows:2;
SELECT VALUE w FROM knows WHERE id < knows:3 ORDER BY id DESC;
SELECT VALUE w FROM knows WHERE in = person:1 ORDER BY id;
DEFINE TABLE edge TYPE RELATION IN person OUT person LIGHTWEIGHT;
RELATE person:1->edge->person:2 RETURN NONE;
RELATE person:2->edge->person:3 RETURN NONE;
RELATE person:3->edge->person:1 RETURN NONE;
SELECT VALUE id FROM edge WHERE id > edge:[person:1, person:2];
SELECT VALUE id FROM edge WHERE id >= edge:[person:1, person:2] AND id <= edge:[person:2, person:3];
SELECT VALUE id FROM edge WHERE id < edge:[person:2, person:3] ORDER BY id DESC;
SELECT VALUE id FROM edge WHERE id > edge:5;
SELECT VALUE id FROM edge WHERE id < edge:zzz;
