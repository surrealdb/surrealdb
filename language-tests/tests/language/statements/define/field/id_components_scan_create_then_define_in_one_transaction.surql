/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	The check of the stored records reads through the transaction of the
	statement, so a record created earlier in the same BEGIN ... COMMIT is
	seen. A block whose record fits the structure commits; one whose record
	does not is refused and rolled back whole, including the table its first
	write created, so reading that table afterwards finds no table at all.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Cannot give `trip` the record id [city: string, time: datetime]: the existing record trip:5 does not match it (the record id is not an array). Copy the records into a table with the new record id instead"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
error = "The table 'trip' does not exist"

[[test.results]]
value = "[1]"
*/

BEGIN;
CREATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = 1 RETURN NONE;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
COMMIT;

BEGIN;
CREATE trip:5 SET temp = 1 RETURN NONE;
DEFINE FIELD id ON trip TYPE [city: string, time: datetime];
COMMIT;

-- The refused block left nothing behind, not even the table its CREATE
-- would have made; the committed one stands.
SELECT VALUE temp FROM trip;
SELECT VALUE temp FROM reading;
