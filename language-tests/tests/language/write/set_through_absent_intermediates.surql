/**
[env]
namespace = true
database = true

[test]
reason = """
What a path-shaped `SET` does when the path does not fully exist yet.

A missing intermediate is created: `SET deep.nested.leaf = 'x'` on a record with
neither `deep` nor `nested` produces both as objects. An intermediate that is
present but nullish is replaced rather than descended into, so `NULL` and a
field removed earlier in this test by `= NONE` both behave as a missing one
does.

An intermediate that holds a non-collection is left alone and the write is
dropped — `SET a.leaf = 'x'` where `a` is `42` changes nothing and raises
nothing. The same silence covers an index part with no element behind it: past
the end of an array, or any of `[0]`, `[$]` and `[*]` over an empty one, for
both `SET` and `UNSET`. None of these is an error, so the only way to observe
the difference between a write that was dropped and one that had no work to do
is the stored record.

These are the branches a data clause hits on partially-shaped records, and the
empty-array and out-of-range cases have no other coverage in the suite.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: viv:1 }]"

[[test.results]]
value = "[{ a: 1, deep: { nested: { leaf: 'x' } }, id: viv:1 }]"

[[test.results]]
value = "[{ a: NULL, id: viv:2 }]"

[[test.results]]
value = "[{ a: { leaf: 'x' }, id: viv:2 }]"

[[test.results]]
value = "[{ a: 1, id: viv:3 }]"

[[test.results]]
value = "[{ id: viv:3 }]"

[[test.results]]
value = "[{ a: { leaf: 'x' }, id: viv:3 }]"

[[test.results]]
value = "[{ a: 42, id: viv:4 }]"

[[test.results]]
value = "[{ a: 42, id: viv:4 }]"

[[test.results]]
value = "[{ id: viv:5, items: [1, 2] }]"

[[test.results]]
value = "[{ id: viv:5, items: [1, 2] }]"

[[test.results]]
value = "[{ id: viv:5, items: [1, 2] }]"

[[test.results]]
value = "[{ id: viv:6, items: [] }]"

[[test.results]]
value = "[{ id: viv:6, items: [] }]"

[[test.results]]
value = "[{ id: viv:6, items: [] }]"

[[test.results]]
value = "[{ id: viv:6, items: [] }]"

[[test.results]]
value = "[{ id: viv:6, items: [] }]"

[[test.results]]
value = "[{ id: viv:6, items: [] }]"

[[test.results]]
value = "[{ id: viv:6, items: [] }]"
*/

-- A missing intermediate is created as an object on the way to the leaf.
DEFINE TABLE viv SCHEMALESS;
CREATE viv:1 SET a = 1;
UPDATE viv:1 SET deep.nested.leaf = 'x';

-- A NULL intermediate is replaced by an object; so is a NONE one.
CREATE viv:2 SET a = NULL;
UPDATE viv:2 SET a.leaf = 'x';
CREATE viv:3 SET a = 1;
UPDATE viv:3 SET a = NONE;
UPDATE viv:3 SET a.leaf = 'x';

-- An intermediate that is neither a collection nor nullish is left alone and
-- the write is dropped.
CREATE viv:4 SET a = 42;
UPDATE viv:4 SET a.leaf = 'x';

-- An index part past the end of an array writes nothing, with or without a
-- field after it.
CREATE viv:5 SET items = [1, 2];
UPDATE viv:5 SET items[5] = 99;
UPDATE viv:5 SET items[5].leaf = 'x';

-- `[0]` and `[$]` over an empty array address no element, so both SET and
-- UNSET are no-ops; `[*]` likewise has nothing to visit.
CREATE viv:6 SET items = [];
UPDATE viv:6 SET items[0] = 1;
UPDATE viv:6 SET items[$] = 1;
UPDATE viv:6 SET items[*] = 1;
UPDATE viv:6 UNSET items[0];
UPDATE viv:6 UNSET items[$];
UPDATE viv:6 UNSET items[*];
