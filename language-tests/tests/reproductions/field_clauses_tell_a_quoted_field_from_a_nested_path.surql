/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A top-level field whose quoted name spells a path (`is.locked`) and the nested field at that path (`is.locked` under `is`) are different fields, and a clause reading one does not read the other. A read records the identifier it reads, so a read of the quoted field records `is.locked`; a read of the nested path records only its root, `is`. Known by the same name, the nested field stands in for the quoted one: an `ASSERT` reading the quoted COMPUTED field no longer has it derived and accepts a locked record, a nested field reading the quoted one is taken to be reading itself, and a cycle through the quoted field goes unrefused."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found NONE for field `guard`, with record `q:locked`, but field must conform to: `is.locked` != true"

[[test.results]]
value = "[{ id: q:open }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ is: { locked: true } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cyclic dependency detected among field clauses: is.locked -> x -> is.locked"
*/

-- The ASSERT reads the quoted COMPUTED field, not the nested one.
DEFINE FIELD `is.locked` ON q COMPUTED status = 'locked';
DEFINE FIELD is.locked ON q TYPE option<bool>;
DEFINE FIELD guard ON q ASSERT `is.locked` != true;
CREATE q:locked SET status = 'locked';
CREATE q:open SET status = 'open' RETURN id;

-- The nested field reads the quoted one, which is not reading itself: the
-- quoted field is derived first.
DEFINE FIELD `is.locked` ON r COMPUTED status = 'locked';
DEFINE FIELD is.locked ON r VALUE `is.locked`;
CREATE r:1 SET status = 'locked' RETURN is;

-- A cycle through the quoted field is refused, whatever the nested field does.
DEFINE FIELD is.locked ON w VALUE 1;
DEFINE FIELD `is.locked` ON w VALUE x;
DEFINE FIELD x ON w VALUE `is.locked`;
