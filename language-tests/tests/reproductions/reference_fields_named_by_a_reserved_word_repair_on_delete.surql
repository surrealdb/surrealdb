/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A reference key names its field as the field is written in SurrealQL, which
	for a reserved word such as `by` is the quoted form. Deleting the record a
	`by` field refers to finds the key and repairs the referencing record by the
	field's `ON DELETE` strategy, for each strategy, rather than failing with
	"The field does not exist".
"""

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: target:cascade }, { id: target:unset }, { id: target:reject }, { id: target:custom }]"

[[test.results]]
value = "[{ by: target:cascade, id: cascaded:1 }]"

[[test.results]]
value = "[{ by: target:unset, id: unset:1 }]"

[[test.results]]
value = "[{ by: target:reject, id: rejected:1 }]"

[[test.results]]
value = "[{ by: target:custom, id: custom:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: unset:1 }]"

[[test.results]]
error = "Cannot delete `target:reject` as it is referenced by `rejected:1` with an ON DELETE REJECT clause"

[[test.results]]
value = "[target:custom, target:reject]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ by: target:custom, id: custom:1, note: 'gone' }]"
*/

DEFINE TABLE target;
DEFINE TABLE cascaded;
DEFINE TABLE unset;
DEFINE TABLE rejected;
DEFINE TABLE custom;
DEFINE FIELD by ON cascaded TYPE option<record<target>> REFERENCE ON DELETE CASCADE;
DEFINE FIELD by ON unset TYPE option<record<target>> REFERENCE ON DELETE UNSET;
DEFINE FIELD by ON rejected TYPE option<record<target>> REFERENCE ON DELETE REJECT;
DEFINE FIELD by ON custom TYPE option<record<target>> REFERENCE ON DELETE THEN (UPDATE $this SET note = 'gone');

CREATE target:cascade, target:unset, target:reject, target:custom;
CREATE cascaded:1 SET by = target:cascade;
CREATE unset:1 SET by = target:unset;
CREATE rejected:1 SET by = target:reject;
CREATE custom:1 SET by = target:custom;

DELETE target:cascade;
SELECT * FROM cascaded;

DELETE target:unset;
SELECT * FROM unset;

DELETE target:reject;
SELECT VALUE id FROM target ORDER BY id;

DELETE target:custom;
SELECT * FROM custom;
