/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "REMOVE FIELD purges the reference keys a REFERENCE field wrote. Re-defining the field writes keys for the links the stored records hold then, not the ones they held before: a record no longer linked can be deleted without acting on a stale reference, and the record still linked carries the field's ON DELETE CASCADE."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:one }]"

[[test.results]]
value = "[{ author: person:one, id: comment:one }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:two }]"

[[test.results]]
value = "[{ author: person:two, id: comment:one }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ author: person:two, id: comment:one }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/

DEFINE FIELD author ON comment TYPE record<person> REFERENCE ON DELETE CASCADE;
CREATE person:one;
CREATE comment:one SET author = person:one;

-- Remove the reference field: the reference key it wrote under person:one is
-- purged as part of REMOVE FIELD.
REMOVE FIELD author ON comment;

-- Point the comment at another person while no field tracks it.
CREATE person:two;
UPDATE comment:one SET author = person:two;
-- Re-define the same reference field: its keys are written for the links the
-- records hold now, so person:one has no incoming reference and deleting it
-- must NOT cascade to comment:one. Without the cleanup, the stale key would
-- still resolve and wrongly delete comment:one.
DEFINE FIELD author ON comment TYPE record<person> REFERENCE ON DELETE CASCADE;
DELETE person:one;
SELECT * FROM comment;
-- The link the comment holds now was given its key, and its cascade.
DELETE person:two;
SELECT * FROM comment;
