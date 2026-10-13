/**
[env]
namespace = true
database = true

[test]
reason = """
`SET` and `UNSET` accept a path, not just a field name, and the four index
parts — `[0]`, `[$]`, a literal position, and `[*]` — each address a different
part of an array.

For `SET`, a path ending in an index part writes the element itself; a path
continuing past it writes a leaf under that element, one or two levels down.
`[*]` applies the write to every element, so one statement writes as many
leaves as the array is long. What happens when the path runs through something
that is not there yet is `set_through_absent_intermediates`, not this file.

For `UNSET`, whether a path removes an element or a field under one is decided
by where the path ends. `UNSET items[0]` removes the element and shortens the
array; `UNSET items[0].tag` removes a field and leaves the element in place;
`UNSET items[*]` empties the array, while `UNSET items[*].n` strips one field
from every element and leaves the elements. An element removed by `[$]` shifts
what `[$]` means for the next statement, so the removals here are ordered
last-then-first deliberately.

These are the primitives a mutation's data clause is built from, and the paths
here are the ones with no other coverage in the suite.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: arr:1, items: [{ n: 1 }, { n: 2 }, { n: 3 }] }]"

[[test.results]]
value = "[{ id: arr:1, items: [{ n: 10 }, { n: 2 }, { n: 3 }] }]"

[[test.results]]
value = "[{ id: arr:1, items: [{ n: 10 }, { n: 2 }, { n: 30 }] }]"

[[test.results]]
value = "[{ id: arr:1, items: [{ n: 10 }, { n: 2 }, { n: 30, tag: 'z' }] }]"

[[test.results]]
value = "[{ id: arr:1, items: [{ n: 10, seen: true }, { n: 2, seen: true }, { n: 30, seen: true, tag: 'z' }] }]"

[[test.results]]
value = "[{ id: arr:2, items: [1, 2, 3] }]"

[[test.results]]
value = "[{ id: arr:2, items: [10, 2, 3] }]"

[[test.results]]
value = "[{ id: arr:2, items: [10, 2, 30] }]"

[[test.results]]
value = "[{ id: arr:2, items: [10, 20, 30] }]"

[[test.results]]
value = "[{ id: arr:2, items: [0, 0, 0] }]"

[[test.results]]
value = "[{ id: arr:3, items: [{ meta: { n: 1 } }, { meta: { n: 2 } }] }]"

[[test.results]]
value = "[{ id: arr:3, items: [{ meta: { n: 10 } }, { meta: { n: 2 } }] }]"

[[test.results]]
value = "[{ id: arr:3, items: [{ meta: { n: 10, seen: true } }, { meta: { n: 2, seen: true } }] }]"

[[test.results]]
value = "[{ id: arr:4, items: [{ n: 1, tag: 'a' }, { n: 2, tag: 'b' }, { n: 3, tag: 'c' }] }]"

[[test.results]]
value = "[{ id: arr:4, items: [{ n: 1 }, { n: 2, tag: 'b' }, { n: 3, tag: 'c' }] }]"

[[test.results]]
value = "[{ id: arr:4, items: [{  }, { tag: 'b' }, { tag: 'c' }] }]"

[[test.results]]
value = "[{ id: arr:4, items: [{  }, { tag: 'b' }] }]"

[[test.results]]
value = "[{ id: arr:4, items: [{ tag: 'b' }] }]"

[[test.results]]
value = "[{ id: arr:4, items: [] }]"
*/

-- An index part addresses an element; a field after it addresses a leaf under
-- that element.
DEFINE TABLE arr SCHEMALESS;
CREATE arr:1 SET items = [{ n: 1 }, { n: 2 }, { n: 3 }];
UPDATE arr:1 SET items[0].n = 10;
UPDATE arr:1 SET items[$].n = 30;
UPDATE arr:1 SET items[2].tag = 'z';
UPDATE arr:1 SET items[*].seen = true;

-- The same four parts writing the element itself rather than a leaf under it.
CREATE arr:2 SET items = [1, 2, 3];
UPDATE arr:2 SET items[0] = 10;
UPDATE arr:2 SET items[$] = 30;
UPDATE arr:2 SET items[1] = 20;
UPDATE arr:2 SET items[*] = 0;

-- A leaf two levels under an index part, and one under `[*]`.
CREATE arr:3 SET items = [{ meta: { n: 1 } }, { meta: { n: 2 } }];
UPDATE arr:3 SET items[0].meta.n = 10;
UPDATE arr:3 SET items[*].meta.seen = true;

-- UNSET removes a field under one element, a field under every element, and
-- then whole elements. Removing `[$]` before `[0]` keeps each removal aimed at
-- the element the comment names.
CREATE arr:4 SET items = [{ n: 1, tag: 'a' }, { n: 2, tag: 'b' }, { n: 3, tag: 'c' }];
UPDATE arr:4 UNSET items[0].tag;
UPDATE arr:4 UNSET items[*].n;
UPDATE arr:4 UNSET items[$];
UPDATE arr:4 UNSET items[0];
UPDATE arr:4 UNSET items[*];
