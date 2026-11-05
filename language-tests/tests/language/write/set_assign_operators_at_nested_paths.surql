/**
[env]
namespace = true
database = true

[test]
reason = """
The four assignment operators a data clause can carry — `=`, `+=`, `-=` and
`+?=` — applied at a nested path rather than at a top-level field.

Each has a distinct contract. `=` writes the value, except that a right-hand
side of `NONE` removes the field instead of storing it. `+=` adds numerically
and appends to an array, keeping duplicates. `-=` subtracts numerically and
removes every occurrence of a value from an array. `+?=` unions: it appends
only what is not already present, and raises `Cannot extend` against a value
that is neither an array nor absent.

The compound operators also decide what to do when the field is absent, and the
answer depends on the right-hand side rather than on the operator: `+= 7`
against a missing field stores the number `7`, while `+= 'x'` stores `['x']`
and `+?= 1` stores `[1]`.

`+?=` reaches only two other files in the suite, and none of the four is
otherwise exercised below the top level of a record.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: { b: { c: 1, d: 2 } }, id: op:1 }]"

[[test.results]]
value = "[{ a: { b: { c: 2, d: 2 } }, id: op:1 }]"

[[test.results]]
value = "[{ a: { b: { d: 2 } }, id: op:1 }]"

[[test.results]]
value = "[{ a: { list: [1, 2], n: 1 }, id: op:2 }]"

[[test.results]]
value = "[{ a: { list: [1, 2], n: 6 }, id: op:2 }]"

[[test.results]]
value = "[{ a: { list: [1, 2, 2], n: 6 }, id: op:2 }]"

[[test.results]]
value = "[{ a: { list: [1, 2, 2, 3, 3], n: 6 }, id: op:2 }]"

[[test.results]]
value = "[{ a: { fresh: 7, list: [1, 2, 2, 3, 3], n: 6 }, id: op:2 }]"

[[test.results]]
value = "[{ a: { fresh: 7, list: [1, 2, 2, 3, 3], n: 6, words: ['x'] }, id: op:2 }]"

[[test.results]]
value = "[{ a: { list: [1, 2, 3, 2], n: 10 }, id: op:3 }]"

[[test.results]]
value = "[{ a: { list: [1, 2, 3, 2], n: 7 }, id: op:3 }]"

[[test.results]]
value = "[{ a: { list: [1, 3], n: 7 }, id: op:3 }]"

[[test.results]]
value = "[{ a: { list: [1, 2] }, id: op:4 }]"

[[test.results]]
value = "[{ a: { list: [1, 2] }, id: op:4 }]"

[[test.results]]
value = "[{ a: { list: [1, 2, 3] }, id: op:4 }]"

[[test.results]]
value = "[{ a: { fresh: [1], list: [1, 2, 3] }, id: op:4 }]"

[[test.results]]
value = "[{ a: { n: 1 }, id: op:5 }]"

[[test.results]]
error = "Cannot extend 'int' as it is not an array"

[[test.results]]
value = "[{ id: op:6, items: [{ list: [1], n: 1 }, { list: [2], n: 2 }] }]"

[[test.results]]
value = "[{ id: op:6, items: [{ list: [1], n: 11 }, { list: [2], n: 2 }] }]"

[[test.results]]
value = "[{ id: op:6, items: [{ list: [1], n: 11 }, { list: [2], n: 1 }] }]"

[[test.results]]
value = "[{ id: op:6, items: [{ list: [1, 9], n: 11 }, { list: [2], n: 1 }] }]"

[[test.results]]
value = "[{ id: op:6, items: [{ list: [0], n: 11 }, { list: [2], n: 1 }] }]"

[[test.results]]
value = "[{ id: op:6, items: [{ list: [0], n: 11 }, { list: [2] }] }]"
*/

-- `=` writes a nested leaf, and `= NONE` removes it rather than storing NONE.
DEFINE TABLE op SCHEMALESS;
CREATE op:1 SET a = { b: { c: 1, d: 2 } };
UPDATE op:1 SET a.b.c = 2;
UPDATE op:1 SET a.b.c = NONE;

-- `+=` at a nested path: numeric addition, array append keeping duplicates,
-- and creation from a field that is not there yet.
CREATE op:2 SET a = { n: 1, list: [1, 2] };
UPDATE op:2 SET a.n += 5;
UPDATE op:2 SET a.list += 2;
UPDATE op:2 SET a.list += [3, 3];
UPDATE op:2 SET a.fresh += 7;
UPDATE op:2 SET a.words += 'x';

-- `-=` at a nested path: numeric subtraction, and removal of every occurrence
-- of an array element.
CREATE op:3 SET a = { n: 10, list: [1, 2, 3, 2] };
UPDATE op:3 SET a.n -= 3;
UPDATE op:3 SET a.list -= 2;

-- `+?=` at a nested path unions where `+=` concatenates.
CREATE op:4 SET a = { list: [1, 2] };
UPDATE op:4 SET a.list +?= 2;
UPDATE op:4 SET a.list +?= [2, 3];
UPDATE op:4 SET a.fresh +?= 1;

-- `+?=` against a value that is neither an array nor absent is an error.
CREATE op:5 SET a = { n: 1 };
UPDATE op:5 SET a.n +?= 2;

-- Each operator at an indexed path, not only a dotted one.
CREATE op:6 SET items = [{ n: 1, list: [1] }, { n: 2, list: [2] }];
UPDATE op:6 SET items[0].n += 10;
UPDATE op:6 SET items[$].n -= 1;
UPDATE op:6 SET items[0].list +?= [9];
UPDATE op:6 SET items[0].list = [0];
UPDATE op:6 SET items[$].n = NONE;
