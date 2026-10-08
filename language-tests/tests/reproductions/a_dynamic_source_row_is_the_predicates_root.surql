/**
[env]
namespace = true
database = true

[test]
reason = """
A `FROM` whose source is an expression the planner cannot read at plan time —
a call, so `array::shuffle(...)` rather than a written array — is scanned
dynamically, and a `WHERE` the planner pushed onto it is judged there, once per
element the source produced.

That element is the row, so it is the predicate's document root as well as its
current value. A path evaluates each step against the value it has reached, so
a read of the *row* inside a path — the separator of `text.split(sep)`, the
index of `a[i]` — is owed the row and not that value. Bound as the current
value alone, `sep` would resolve against the string `text` had reached and arrive
as `NONE`, so the call would raise an argument-type error; the row must be
retained.

Both source shapes are covered, since each judges its predicate in its own
place: an array source filters its elements, a scalar source judges the single
row. The literal-array control is the same predicate over a source the planner
*can* read, which never took this path.
"""

[[test.results]]
value = "[{ sep: '-', text: 'a-b' }]"

[[test.results]]
value = "[{ sep: '-', text: 'a-b' }]"

[[test.results]]
value = "[{ a: [1, 2, 3], i: 1 }]"

[[test.results]]
value = "[{ sep: '-', text: 'a-b' }]"
*/

-- An array source: the predicate judges each element.
SELECT * FROM array::shuffle([{ text: 'a-b', sep: '-' }]) WHERE text.split(sep) = ['a', 'b'];

-- A scalar source: the predicate judges the single row.
SELECT * FROM array::shuffle([{ text: 'a-b', sep: '-' }])[0] WHERE text.split(sep) = ['a', 'b'];

-- A row read from inside a path, with no method involved.
SELECT * FROM array::shuffle([{ a: [1, 2, 3], i: 1 }])[0] WHERE a[i] = 2;

-- Control: the same predicate over a source the planner reads at plan time.
SELECT * FROM [{ text: 'a-b', sep: '-' }] WHERE text.split(sep) = ['a', 'b'];
