/**
[env]
namespace = true
database = true

[test]
reason = """
A field's declared kind is enforced on write alone, so a value stored before
the declaration — or before `DEFINE FIELD OVERWRITE` retyped it — holds what it
held. `items.b.len()` over an array of objects maps `.b.len()` over the
elements; reading the declaration as proof that `items` is scalar drops the
broadcast and evaluates `len()` once, over the combined `items.b`, so `[1, 2]`
becomes `2`.

Broadcast resolution therefore reads the base's kind the same way cast elision
reads its operand's: through `checked_kind`, which answers for a kind the value
is known to have rather than one it is declared to have.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[1, 2]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[1, 2]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[1, 2]]"
*/

DEFINE TABLE t SCHEMALESS;
DEFINE FIELD items ON t TYPE array<object>;
CREATE t:1 SET items = [{ b: [1] }, { b: [1, 2] }] RETURN NONE;

-- The declaration still matches what is stored.
SELECT VALUE items.b.len() FROM t:1;

-- Retyped under the stored value, which keeps its array shape.
DEFINE FIELD OVERWRITE items ON t TYPE object;
SELECT VALUE items.b.len() FROM t:1;

-- The same hazard without `OVERWRITE`: the value is written before the field
-- is declared, so the declaration never judged it.
DEFINE TABLE u SCHEMALESS;
CREATE u:1 SET items = [{ b: [1] }, { b: [1, 2] }] RETURN NONE;
DEFINE FIELD items ON u TYPE object;
SELECT VALUE items.b.len() FROM u:1;
