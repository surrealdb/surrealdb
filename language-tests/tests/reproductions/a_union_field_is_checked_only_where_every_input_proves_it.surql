/**
[env]
namespace = true
database = true

[test]
reason = """
A union's schema merged each field from the first input that declared it and
recomputed only its kind, so `checked` was inherited. A coerced column on one
input then claimed the stored field another input reads was known to hold its
declared kind, and the `kind` rule dropped a cast that was doing real work.

`first.a` is `COMPUTED`, so a layer coerces it as it lands and its kind is
checked. `second.a` is a stored field retyped after the fact, so it holds the
string it was written with and its kind is declared alone. With `first` named
first, `<int> a` was read as a cast to the kind `a` already had, dropped, and
`second:1` compared `'7'` against `7` — the row was silently lost. Naming
`second` first inherited `checked = false` and kept the cast, which is what
made the answer depend on the order of the sources.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: '7', id: second:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 7, id: first:1 }]"

[[test.results]]
value = "[{ id: first:1 }, { id: second:1 }]"

[[test.results]]
value = "[{ id: second:1 }, { id: first:1 }]"

[[test.results]]
value = "[{ id: second:1 }]"
*/

DEFINE TABLE second SCHEMAFULL;
DEFINE FIELD a ON second TYPE any;
CREATE second:1 SET a = "7";
DEFINE FIELD OVERWRITE a ON second TYPE int;

DEFINE TABLE first SCHEMAFULL;
DEFINE FIELD a ON first TYPE int COMPUTED 7;
CREATE first:1;

-- Both sources answer the cast, in either order.
SELECT id FROM first, second WHERE <int> a = 7;
SELECT id FROM second, first WHERE <int> a = 7;

-- The stored field alone, for the reading the union must not contradict.
SELECT id FROM second WHERE <int> a = 7;
