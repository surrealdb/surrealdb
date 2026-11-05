/**
[env]
namespace = true
database = true

[test]
reason = """
A field's DEFAULT / VALUE / ASSERT clause can read other fields of the same
record, so the field being read has to be processed first. The field list is
loaded in name order, which bears no relation to that, so the write path derives
the order from the dependencies instead.

The two pairs of tables here are identical but for their field names, chosen so
that name order and dependency order disagree. Each pair must produce the same
result, which is the whole point: renaming a field cannot change what a clause
computes.

`v2` is the case that shows this matters beyond COMPUTED fields: no computed
field is involved anywhere, `a_dep` simply reads `z_src`, and name order alone
would put the reader first.

For a COMPUTED field the ordering also decides *which* value a clause sees. Field
clauses run after the record's new values are in place, so a computed field read
from one is derived from those new values: `age` is 18 by then, so `z_val` and
`a_val` are both true. That is the opposite end of the statement from the data
clause, which reads the pre-mutation snapshot and sees `age` as 17.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a_src: 10, b_dep: 11, id: v1:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a_dep: 11, id: v2:1, z_src: 10 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a_comp: false, age: 17, id: x:1, z_val: false }]"

[[test.results]]
value = "[{ a_comp: true, age: 18, id: x:1, z_val: true }]"

[[test.results]]
value = "[{ a_comp: true, age: 18, z_val: true }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a_val: false, age: 17, id: y:1, z_comp: false }]"

[[test.results]]
value = "[{ a_val: true, age: 18, id: y:1, z_comp: true }]"

[[test.results]]
value = "[{ a_val: true, age: 18, z_comp: true }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 5, dbl: 10, id: st:1 }]"

[[test.results]]
value = "[{ a: 5, dbl: 10, id: st:1 }]"

[[test.results]]
value = "[{ a: 5, dbl: 10 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cyclic dependency detected among field clauses: p -> q -> p"
*/

-- No COMPUTED fields: one VALUE clause reads another field's VALUE output.
DEFINE TABLE v1 SCHEMALESS;
DEFINE FIELD a_src ON v1 TYPE option<number> VALUE 10;
DEFINE FIELD b_dep ON v1 TYPE option<number> VALUE a_src + 1;
CREATE v1:1;
DEFINE TABLE v2 SCHEMALESS;
DEFINE FIELD z_src ON v2 TYPE option<number> VALUE 10;
DEFINE FIELD a_dep ON v2 TYPE option<number> VALUE z_src + 1;
CREATE v2:1;

-- A VALUE clause reading a COMPUTED field, with the reader named after the
-- computed field in one table and before it in the other.
DEFINE TABLE x SCHEMALESS;
DEFINE FIELD age    ON x TYPE number;
DEFINE FIELD z_val  ON x TYPE option<bool> VALUE a_comp;
DEFINE FIELD a_comp ON x COMPUTED age >= 18;
CREATE x:1 SET age = 17;
UPDATE x:1 SET age = 18 WHERE true;
SELECT age, a_comp, z_val FROM x:1;

DEFINE TABLE y SCHEMALESS;
DEFINE FIELD age    ON y TYPE number;
DEFINE FIELD z_comp ON y COMPUTED age >= 18;
DEFINE FIELD a_val  ON y TYPE option<bool> VALUE z_comp;
CREATE y:1 SET age = 17;
UPDATE y:1 SET age = 18 WHERE true;
SELECT age, z_comp, a_val FROM y:1;

-- A computed field is still never stored, and a value assigned to one is still
-- discarded, whichever order the fields were processed in.
DEFINE TABLE st SCHEMALESS;
DEFINE FIELD a   ON st TYPE number;
DEFINE FIELD dbl ON st COMPUTED a * 2;
CREATE st:1 SET a = 5;
UPDATE st:1 SET dbl = 99 WHERE true;
SELECT a, dbl FROM st:1;

-- Clauses that read each other in a cycle have no evaluation order, and are
-- rejected when the second field is defined rather than when a record is written.
DEFINE TABLE cyc SCHEMALESS;
DEFINE FIELD p ON cyc TYPE option<number> VALUE q;
DEFINE FIELD q ON cyc TYPE option<number> VALUE p;
