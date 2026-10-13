/**
[env]
namespace = true
database = true

[test]
reason = """
Every expression in an updating statement's `WHERE` condition and data clause is
evaluated against one pre-mutation snapshot of the record, so no read observes
the statement's own writes.

Two consequences are pinned here. Plain fields read their old value, which is
what makes `SET a = a + 1, b = a + 1` assign `b` from the old `a` regardless of
assignment order. `COMPUTED` fields are part of that snapshot too, so a computed
field reads the same value in the data clause as in the `WHERE` condition, and
reads the same whether or not the statement has a `WHERE` condition at all: the
three `person` statements differ only in assignment order and in the presence of
a condition, and all three must assign `my_field = false` even though the same
statement sets `age = 18`.

After the write the computed field is re-derived from the stored values, so
`can_drive` reads `true` on the record the statement returns.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: 1, id: snap:1 }]"

[[test.results]]
value = "[{ a: 2, b: 2, id: snap:1 }]"

[[test.results]]
value = "[{ a: 1, b: 1, id: snap:2 }]"

[[test.results]]
value = "[{ a: 2, b: 2, id: snap:2 }]"

[[test.results]]
value = "[{ a: 1, b: 1, id: snap:3 }]"

[[test.results]]
value = "[{ a: 2, b: 2, id: snap:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ age: 17, can_drive: false, id: person:1 }]"

[[test.results]]
value = "[{ age: 18, can_drive: true, id: person:1, my_field: false }]"

[[test.results]]
value = "[{ age: 17, can_drive: false, id: person:2 }]"

[[test.results]]
value = "[{ age: 18, can_drive: true, id: person:2, my_field: false }]"

[[test.results]]
value = "[{ age: 17, can_drive: false, id: person:3 }]"

[[test.results]]
value = "[{ age: 18, can_drive: true, id: person:3, my_field: false }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 5, c1: 10, c2: 11, id: chain:1 }]"

[[test.results]]
value = "[{ a: 5, c1: 10, c2: 11, copied: 11, id: chain:1 }]"

[[test.results]]
value = "[{ a: 5, c1: 10, c2: 11, id: chain:2 }]"

[[test.results]]
value = "[{ a: 5, c1: 10, c2: 11, copied: 11, id: chain:2 }]"
*/

-- A read in the data clause never observes an assignment made by the same
-- statement, in either assignment order, with or without a condition.
DEFINE TABLE snap SCHEMALESS;
CREATE snap:1 SET a = 1, b = 1;
UPDATE snap:1 SET a = a + 1, b = a + 1;
CREATE snap:2 SET a = 1, b = 1;
UPDATE snap:2 SET b = a + 1, a = a + 1;
CREATE snap:3 SET a = 1, b = 1;
UPDATE snap:3 SET a = a + 1, b = a + 1 WHERE true;

-- A COMPUTED field is read from the same snapshot: `can_drive` is false in the
-- data clause and in the condition even though `age` is being set to 18, and it
-- is re-derived to true for the returned record.
DEFINE TABLE person SCHEMALESS;
DEFINE FIELD can_drive ON person COMPUTED age >= 18;
CREATE person:1 SET age = 17;
UPDATE person:1 SET age = 18, my_field = can_drive;
CREATE person:2 SET age = 17;
UPDATE person:2 SET my_field = can_drive, age = 18;
CREATE person:3 SET age = 17;
UPDATE person:3 SET my_field = can_drive, age = 18 WHERE can_drive = false;

-- A computed field defined in terms of another computed field resolves through
-- the whole chain, identically with and without a condition.
DEFINE TABLE chain SCHEMALESS;
DEFINE FIELD c1 ON chain COMPUTED a * 2;
DEFINE FIELD c2 ON chain COMPUTED c1 + 1;
CREATE chain:1 SET a = 5;
UPDATE chain:1 SET copied = c2;
CREATE chain:2 SET a = 5;
UPDATE chain:2 SET copied = c2 WHERE true;
