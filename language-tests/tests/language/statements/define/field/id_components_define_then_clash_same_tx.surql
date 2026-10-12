/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	The clash check reads the catalog through the transaction of the statement,
	so a field defined under the name of a component in the same BEGIN ... COMMIT
	as the record id that names it is refused, and the block is rolled back
	whole: nothing it defined is left to clash with a later definition.
"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Cannot define the field `city` on `reading`: `city` is part of the record id of `reading`"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define the field `city` on `reading`: `city` is part of the record id of `reading`"
*/

BEGIN;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
DEFINE FIELD city ON reading TYPE string;
COMMIT;

-- The rolled-back block left no record id behind to clash with.
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
DEFINE FIELD city ON reading TYPE string;
