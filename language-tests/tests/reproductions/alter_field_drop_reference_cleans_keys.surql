/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "ALTER FIELD ... DROP REFERENCE must purge the reference keys the field wrote. Otherwise the keys are stranded (the DELETE purge gate skips the scan because no current field targets the table), and re-adding the reference would let a later DELETE find the stale key and fire the wrong ON DELETE CASCADE. Re-adding it writes keys for the links the records hold then: comment:one survives deleting person:one, which it no longer links to, and goes with person:two, which it does."

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

-- Drop the REFERENCE clause. This must purge the reference key written under
-- person:one; the field itself (record<person>) remains.
ALTER FIELD author ON comment DROP REFERENCE;

-- Point the comment at another person while no reference is tracked.
CREATE person:two;
UPDATE comment:one SET author = person:two;
-- Re-add the reference. The stored records are scanned for the links they hold
-- now, so a key is written for comment:one -> person:two and none for
-- comment:one -> person:one.
DEFINE FIELD OVERWRITE author ON comment TYPE record<person> REFERENCE ON DELETE CASCADE;

-- With the stale key purged at DROP time, deleting person:one must NOT cascade
-- into comment:one. (Without the cleanup the stale key would survive and drive
-- a wrong cascade here, deleting comment:one.)
DELETE person:one;
SELECT * FROM comment;
-- The link the comment holds now was given its key, and its cascade.
DELETE person:two;
SELECT * FROM comment;
