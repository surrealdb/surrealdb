/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	Removing a table in a transaction removes its records, and a `DEFINE INDEX`
	without `CONCURRENTLY` builds from the table as last committed, which still
	holds them. The definition is refused when the removal took a record with it,
	and the transaction keeps nothing. A table that held no record has none to
	remove, so the table defined again takes the index.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Cannot define index `email` on `removed`: this transaction has written records of `removed`, which a build without CONCURRENTLY cannot see. Define the index CONCURRENTLY, or commit those writes first."

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "[{ email: 'removed@example.com', id: removed:1 }]"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { email: 'DEFINE INDEX email ON emptied FIELDS email' }, lives: {  }, tables: {  } }"
*/

-- A table that held a record.
CREATE removed:1 SET email = 'removed@example.com' RETURN NONE;
BEGIN;
REMOVE TABLE removed;
DEFINE TABLE removed SCHEMALESS;
DEFINE INDEX email ON removed FIELDS email;
COMMIT;
SELECT * FROM removed;
INFO FOR TABLE removed;

-- A table that held none.
DEFINE TABLE emptied SCHEMALESS;
BEGIN;
REMOVE TABLE emptied;
DEFINE TABLE emptied SCHEMALESS;
DEFINE INDEX email ON emptied FIELDS email;
COMMIT;
INFO FOR TABLE emptied;
