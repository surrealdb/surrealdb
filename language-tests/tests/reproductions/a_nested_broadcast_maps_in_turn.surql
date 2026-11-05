/**
[env]
namespace = true
database = true

[test]
reason = """
A mapping step over an array applies the rest of the path to each element. That
nests: `rows.a.b[0]` maps `.a.b[0]` over `rows`, and inside that maps `.b[0]`
over each `a`, so `[0]` indexes each element's own `b`. Running the inner
mapping as a plain sequence instead applied the positional step to the array
the `.b` step had collected — one level too high.

The bracket ends read a collection, so a value that is not one has no end and
answers `NONE`. `[0]` already did; `[$]` passed the value through. The
`first()` and `last()` methods are registered functions and raise for such a
value instead, which is a different spelling and a different answer.
"""

[[test.results]]
value = "[[1]]"

[[test.results]]
value = "[[NONE, NONE]]"

[[test.results]]
value = "[[1, 2]]"

[[test.results]]
value = "[[NONE, NONE]]"

[[test.results]]
value = "[1, 3]"

[[test.results]]
value = "[[1, 2], [3, 4]]"

[[test.results]]
value = "[1, 3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "8"

[[test.results]]
error = "There was a problem running the last() function. no such method found for the string type"
*/

-- The inner mapping indexes each element's own `b`.
RETURN { rows: [{ a: [{ b: [1, 2] }] }] }.rows.a.b[0];
RETURN [{ a: [{ b: 1 }, { b: 2 }] }].a.b[0];
RETURN [{ a: [{ b: [1, 9] }, { b: [2, 8] }] }].a.b[0];
RETURN [{ a: [{ b: 1 }, { b: 2 }] }].a.b[$];

-- One level of mapping is unchanged.
RETURN { rows: [{ a: [1, 2] }, { a: [3, 4] }] }.rows.a[0];
RETURN { rows: [{ a: [1, 2] }, { a: [3, 4] }] }.rows.a;

-- A non-array base runs the body once, and the body maps inside it.
RETURN { row: { a: [{ b: [1, 2] }, { b: [3] }] } }.row.a.b[0];

-- A value that is not a collection has no end to read.
RETURN 1[$];
RETURN 'ab'[$];
RETURN { a: 1 }[$];
RETURN [7, 8][$];

-- The method spelling is a registered function and raises instead.
RETURN ('ab').last();
