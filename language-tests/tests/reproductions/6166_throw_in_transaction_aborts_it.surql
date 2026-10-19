/**
[env]
namespace = true
database = true

[test]
reason = """
THROW inside BEGIN … COMMIT is an error like any other, so it aborts the whole
transaction: nothing the block wrote is kept, the statements around the THROW
report the failed transaction, and COMMIT is refused. This is by design (the
CANCEL docs direct conditional abort to THROW). To skip writes without an
error, guard them with IF and let the block COMMIT, as the second block does.
"""
issue = 6166

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: properties:ShouldExist }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "An error occurred: GRACEFUL"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "[properties:ShouldExist]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: properties:Kept }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[properties:Kept, properties:ShouldExist]"
*/

DEFINE TABLE properties SCHEMAFULL PERMISSIONS NONE;
CREATE properties:ShouldExist;
BEGIN TRANSACTION;
CREATE properties:NotExist;
IF true { THROW "GRACEFUL" };
SELECT * FROM properties;
COMMIT TRANSACTION;
SELECT VALUE id FROM properties;
BEGIN TRANSACTION;
CREATE properties:Kept;
IF false { CREATE properties:NotWritten };
COMMIT TRANSACTION;
SELECT VALUE id FROM properties ORDER BY id;
