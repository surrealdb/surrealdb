/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "After REMOVE FIELD (which purges the reference keys it wrote), deleting the formerly-referenced record succeeds and does not cascade, since the field — and thus its ON DELETE strategy — is gone."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:one }]"

[[test.results]]
value = "[{ author: person:one, id: comment:one }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ author: person:one, id: comment:one }]"

*/

DEFINE FIELD author ON comment TYPE record<person> REFERENCE ON DELETE CASCADE;
CREATE person:one;
CREATE comment:one SET author = person:one;

-- Remove the reference field. This purges the reference key it wrote under
-- `person:one`, so no orphaned key is left for a later delete to trip over.
REMOVE FIELD author ON comment;

-- No REFERENCE field can target `person` anymore, so the delete takes the skip
-- path and succeeds. With the field gone there is no ON DELETE CASCADE to fire,
-- so the comment is retained.
DELETE person:one;
SELECT * FROM person;
SELECT * FROM comment;
