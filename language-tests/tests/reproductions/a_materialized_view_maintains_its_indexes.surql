/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "An index defined on an ungrouped `DEFINE TABLE … AS SELECT` table was built by the view's backfill and never maintained afterwards, because the ungrouped view arm wrote its row with `tx.set_record` and ran none of the view table's lifecycle. A missing index entry is invisible to an ordinary read — it falls back to a scan and returns the same rows — so `UNIQUE` is the observable: it is the one index whose absence changes an answer rather than a plan."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ field: 7, id: target:1 }]"

[[test.results]]
error = "Database index `unique_field` already contains 7, with record `mirror:1`"

[[test.results]]
value = "[{ field: 7, id: mirror:1 }]"

*/

DEFINE TABLE target SCHEMALESS;
DEFINE TABLE mirror AS SELECT field FROM target;
DEFINE INDEX unique_field ON mirror FIELDS field UNIQUE;

CREATE target:1 SET field = 7;

-- A second base record projects the same view row value. The view table's
-- unique index is maintained by the write that maintains the view, so the
-- duplicate is refused rather than silently stored.
CREATE target:2 SET field = 7;

SELECT * FROM mirror ORDER BY id;
