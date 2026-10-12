/**
[env]
namespace = true
database = true

[test]
reason = """
	Defining a structure equal to the stored one does not scan the records
	again, but removing the names and defining them again inside one
	transaction does: the second definition is judged against the catalog as
	the transaction holds it, which has no names, so every record is read. A
	record that does not fit refuses the definition and the whole transaction,
	and the table keeps its names and its records.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ city: 'a', id: reading:['a', 1], n: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Cannot give `reading` the record id [city: string, n: int]: the existing record reading:['b', 'x'] does not match it (`n` is not an int). Copy the records into a table with the new record id instead"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "[{ city: 'a', id: reading:['a', 1], n: 1 }]"

[[test.results]]
value = "{ events: {  }, fields: { id: 'DEFINE FIELD id ON reading TYPE [city: string, n: int] PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"
*/

DEFINE FIELD id ON reading TYPE [city: string, n: int];
CREATE reading SET city = 'a', n = 1 RETURN AFTER;
BEGIN;
REMOVE FIELD id ON reading;
CREATE reading:['b', 'x'] SET v = 1 RETURN NONE;
DEFINE FIELD id ON reading TYPE [city: string, n: int];
COMMIT;
SELECT * FROM reading;
INFO FOR TABLE reading;
