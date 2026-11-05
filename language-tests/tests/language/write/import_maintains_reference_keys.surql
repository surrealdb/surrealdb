/**
[env]
namespace = true
database = true

[test]
reason = "A replay under OPTION IMPORT skips the state a record's own field clauses would derive — TYPE, VALUE, ASSERT — because the exported record already carries their result. A reference key is not that: the backlink lives under the *referenced* record's range, so no exported record carries it and nothing else in the replay writes it. Skipping reference maintenance on import therefore drops every backlink in a restored database, silently, with the referencing field itself intact — a read of the record looks correct and only a `<~` traversal shows the loss. The keys must be maintained here."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[holder:1]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[]]"
*/

OPTION IMPORT;
DEFINE TABLE target SCHEMALESS;
DEFINE TABLE holder SCHEMALESS;
DEFINE FIELD link ON holder TYPE record<target> REFERENCE;
CREATE target:1;
-- The write an export replays: the referencing record, carrying its link.
CREATE holder:1 SET link = target:1;
-- The backlink it must have published under `target`'s range.
SELECT VALUE <~holder FROM target;
-- Releasing it is import's job too: a delete under replay must take the key
-- back, or the restored database answers a link to a record it no longer has.
OPTION IMPORT = false;
DELETE holder:1;
SELECT VALUE <~holder FROM target;
