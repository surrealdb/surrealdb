/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	The schema statements of a `BEGIN` … `COMMIT` group that fails or is
	cancelled are rolled back with it. Inside the group a statement reads
	what an earlier statement of the group defined; a statement after the
	group reads the catalog as it was before the group, and a group that
	commits leaves its definitions for every statement after it.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "An error occurred: stop"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "1"

[[test.results]]
value = "[1]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "4"

[[test.results]]
value = "[4]"
*/

DEFINE FUNCTION fn::v() { RETURN 1 };
DEFINE TABLE t SCHEMALESS;
DEFINE FIELD c ON t COMPUTED 1;
CREATE t:1 RETURN NONE;

BEGIN;
DEFINE FUNCTION OVERWRITE fn::v() { RETURN 2 };
DEFINE FIELD OVERWRITE c ON t COMPUTED 2;
fn::v();
SELECT VALUE c FROM t;
THROW 'stop';
COMMIT;

fn::v();
SELECT VALUE c FROM t;

BEGIN;
DEFINE FUNCTION OVERWRITE fn::v() { RETURN 3 };
CANCEL;

fn::v();

BEGIN;
DEFINE FUNCTION OVERWRITE fn::v() { RETURN 4 };
DEFINE FIELD OVERWRITE c ON t COMPUTED 4;
COMMIT;

fn::v();
SELECT VALUE c FROM t;
