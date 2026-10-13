/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A field's DEFAULT, VALUE and ASSERT clauses read a field whose name has to be quoted (`is-locked`, `z-src`) the way they read any other: processed after it, and with a COMPUTED one derived first. A read records the unescaped identifier, so the write path's field graph has to know each field by that name. Keyed by its escaped form, the field is neither ordered before nor materialised for the clauses reading it: they read NONE, and an assertion written to deny (`ASSERT `is-locked` != true`) accepts a locked record. The same graph decides definition-time cycles, so a cycle among quoted fields is refused and a clause reading its own quoted field is not a cycle."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found NONE for field `guard`, with record `q:locked`, but field must conform to: `is-locked` != true"

[[test.results]]
value = """[{ copy: false, dflt: false, id: q:open, "is-locked": false, status: 'open' }]"""

[[test.results]]
value = "[{ copy: false, dflt: false, id: q:open }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '[{ "a-dep": 11, "z-src": 10 }]'

[[test.results]]
value = "NONE"

[[test.results]]
value = '[{ "n-x": 5 }]'

[[test.results]]
value = '[{ "n-x": 0 }]'

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cyclic dependency detected among field clauses: c-1 -> c-2 -> c-1"

*/

-- A COMPUTED field with a quoted name, read by an ASSERT, a VALUE and a
-- DEFAULT on other fields.
DEFINE FIELD `is-locked` ON q COMPUTED status = 'locked';
DEFINE FIELD guard ON q ASSERT `is-locked` != true;
DEFINE FIELD copy ON q VALUE `is-locked`;
DEFINE FIELD dflt ON q DEFAULT `is-locked`;
-- The assertion rejects the locked record.
CREATE q:locked SET status = 'locked';
CREATE q:open SET status = 'open';
SELECT id, copy, dflt FROM q;

-- Stored fields with quoted names, where name order puts the reader first.
DEFINE FIELD `a-dep` ON r VALUE `z-src` + 1;
DEFINE FIELD `z-src` ON r VALUE 10;
CREATE r:1 RETURN `a-dep`, `z-src`;

-- A clause reading its own quoted field reads the value under consideration;
-- that is not a cycle.
DEFINE FIELD `n-x` ON s VALUE `n-x` ?? 0;
CREATE s:1 SET `n-x` = 5 RETURN `n-x`;
CREATE s:2 RETURN `n-x`;

-- Two quoted fields producing their values from each other have no order.
DEFINE FIELD `c-1` ON w VALUE `c-2`;
DEFINE FIELD `c-2` ON w VALUE `c-1`;
