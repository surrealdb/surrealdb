/**
[env]
namespace = true
database = true

[test]
reason = """
	A write planned in the same transaction, block or loop as the DEFINE FIELD
	that names the components of its table's record id is planned before that
	definition has run, and must still treat the record as the definition says
	it will: the key is made of the components, and the fields that name them
	are held to it and stored in no record. A DEFAULT on the `id` field is read
	the same way. Each table is retyped to an id with no names afterwards, which
	shows the stored record as it is: it holds no copy of a component.
"""

# The setup.
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

# A transaction.
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[reading:['London', d'2024-01-01T00:00:00Z']]"

[[test.results]]
value = "NONE"

# A block that creates and then updates the record it defined the key for.
[[test.results]]
value = "[trip:['London', d'2024-01-01T00:00:00Z']]"

# A block whose definition holds the DEFAULT the key is made from.
[[test.results]]
value = "[visit:['Rome', d'2024-01-01T00:00:00Z']]"

# A loop whose iterations define the id and write.
[[test.results]]
value = "NONE"

# The stored records, with the names taken off the id.
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: reading:['London', d'2024-01-01T00:00:00Z'], temp: 1 }]"

[[test.results]]
value = "[{ id: trip:['London', d'2024-01-01T00:00:00Z'], temp: 2 }]"

[[test.results]]
value = "[{ id: visit:['Rome', d'2024-01-01T00:00:00Z'], temp: 3 }]"

[[test.results]]
value = "[{ id: lap:['Oslo', d'1970-01-02T00:00:00Z'], n: 1 }, { id: lap:['Oslo', d'1970-01-03T00:00:00Z'], n: 2 }]"
*/

DEFINE TABLE reading SCHEMALESS;
DEFINE TABLE trip SCHEMALESS;
DEFINE TABLE visit SCHEMALESS;
DEFINE TABLE lap SCHEMALESS;

BEGIN;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
CREATE reading SET city = 'London', time = d'2024-01-01T00:00:00Z', temp = 1 RETURN VALUE id;
COMMIT;

{
	DEFINE FIELD id ON trip TYPE [city: string, time: datetime];
	CREATE trip SET city = 'London', time = d'2024-01-01T00:00:00Z', temp = 1;
	UPDATE trip SET temp = 2 RETURN VALUE id;
};

{
	DEFINE FIELD id ON visit TYPE [city: string, time: datetime] DEFAULT ['Rome', d'2024-01-01T00:00:00Z'];
	CREATE visit SET temp = 3 RETURN VALUE id;
};

FOR $i IN [1, 2] {
	DEFINE FIELD OVERWRITE id ON lap TYPE [city: string, time: datetime];
	CREATE lap SET city = 'Oslo', time = time::from_secs(86400 * $i), n = $i;
};

DEFINE FIELD OVERWRITE id ON reading TYPE [string, datetime];
DEFINE FIELD OVERWRITE id ON trip TYPE [string, datetime];
DEFINE FIELD OVERWRITE id ON visit TYPE [string, datetime];
DEFINE FIELD OVERWRITE id ON lap TYPE [string, datetime];
SELECT * FROM reading;
SELECT * FROM trip;
SELECT * FROM visit;
SELECT * FROM lap ORDER BY n;
