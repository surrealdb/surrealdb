/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A field's `VALUE` and `ASSERT` decide what a record holds and whether it may
be written, so a clause the planner cannot compile has to be
evaluated rather than refused: declaring one must not make the table
unwritable. A mock record set in scalar position is such a clause — it has no
logical form — and reaching it here proves the clause still governs the write.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: v:1, n: 6 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: a:1, n: 11 }]"

[[test.results]]
error = "Found 3 for field `n`, with record `a:2`, but field must conform to: IF false { |sink:1| } ELSE { $value > 10 }"
*/

DEFINE TABLE sink SCHEMALESS;

-- A `VALUE` whose text holds a mock record set in scalar position.
DEFINE TABLE v SCHEMALESS;
DEFINE FIELD n ON v TYPE option<int> VALUE IF false { |sink:1| } ELSE { $value * 2 };

CREATE v:1 SET n = 3;

-- And the same shape as an `ASSERT`, which still refuses the record it judges.
DEFINE TABLE a SCHEMALESS;
DEFINE FIELD n ON a TYPE int ASSERT IF false { |sink:1| } ELSE { $value > 10 };

CREATE a:1 SET n = 11;
CREATE a:2 SET n = 3;
