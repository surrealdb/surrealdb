/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	An `OMIT` path names a location to remove, and the projection walks it
	structurally: it follows field accesses, `.*` fan-out, literal indices and
	destructures without evaluating any step. That reaches what a written-down
	path names, and nothing else — a filter, a parameter index and a method
	call all name their location only once evaluated.

	A path the walk cannot reach is evaluated instead, and the removal happens.
	Removal by index is a removal: the element leaves the array, rather than
	being left in place as `NONE`. A destructure entry naming the contents of a
	field (`{ c.* }`) empties that field instead of removing it, and one naming
	an expression is rejected, as it is anywhere else. A rest entry (`...`)
	names every field the pattern did not already claim rather than one
	location, so it is rejected the same way.
"""

[[test.results]]
value = "[{ tags: [1] }]"

[[test.results]]
value = "[{ tags: [] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ tags: [1, 3] }]"

[[test.results]]
value = "[{ tags: [2, 3] }]"

[[test.results]]
value = "[{ tags: [2] }]"

[[test.results]]
value = '[{ a: { "1": 2 } }]'

[[test.results]]
value = "[{ tags: [1, 2] }]"

[[test.results]]
value = "[{ a: { c: { d: 2 } } }]"

[[test.results]]
value = "[{ a: { b: 1, c: {  } } }]"

[[test.results]]
error = "An aliased destructuring method is not supported here"

[[test.results]]
value = "[{ a: { b: { y: 2 } } }]"

[[test.results]]
value = "[{ tags: [1, 2] }]"

[[test.results]]
value = "[[1]]"

[[test.results]]
value = "[{ tags: [] }]"

[[test.results]]
error = "A rest destructuring method is not supported here"
*/

-- A filter selects the elements to remove.
SELECT * OMIT tags[WHERE $this > 1] FROM [{ tags: [1, 2, 3] }];

-- So does one whose predicate holds for every element.
SELECT * OMIT tags[WHERE true] FROM [{ tags: [1, 2, 3] }];

-- A parameter names the index only once read.
LET $i = 1;
SELECT * OMIT tags[$i] FROM [{ tags: [1, 2, 3] }];

-- A literal index removes the element it numbers, leaving no hole behind,
-- and each `OMIT` path applies to what the one before it left.
SELECT * OMIT tags[0] FROM [{ tags: [1, 2, 3] }];
SELECT * OMIT tags[0], tags[1] FROM [{ tags: [1, 2, 3] }];

-- On an object the same step spells a key.
SELECT * OMIT a[0] FROM [{ a: { "0": 1, "1": 2 } }];

-- `[$]` is the last element.
SELECT * OMIT tags[$] FROM [{ tags: [1, 2, 3] }];

-- A destructure entry naming a field removes it; one naming its contents
-- empties it.
SELECT * OMIT a.{ b } FROM [{ a: { b: 1, c: { d: 2 } } }];
SELECT * OMIT a.{ c.* } FROM [{ a: { b: 1, c: { d: 2 } } }];

-- An aliased entry names an expression rather than a location.
SELECT * OMIT a.{ b: c } FROM [{ a: { b: 1, c: 2 } }];

-- A destructure that is not the last step fans the remaining steps out over
-- the fields it lists.
SELECT * OMIT a.{ b }.x FROM [{ a: { b: { x: 1, y: 2 } } }];

-- A method call answers with a value, not a location.
SELECT * OMIT tags.len() FROM [{ tags: [1, 2] }];

-- The same declines hold where the projection is a `SELECT VALUE`, and
-- beneath paging.
SELECT VALUE tags OMIT tags[WHERE $this > 1] FROM [{ tags: [1, 2, 3] }];
SELECT * OMIT tags[WHERE true] FROM [{ tags: [1, 2] }] LIMIT 5;

-- A rest entry names every remaining field rather than one location.
SELECT * OMIT a.{ b, ... } FROM [{ a: { b: 1, c: 2 } }];
