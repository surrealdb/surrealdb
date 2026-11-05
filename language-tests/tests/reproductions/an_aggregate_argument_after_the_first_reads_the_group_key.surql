/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	An aggregate's arguments after the first describe the group rather than a
	row — the separator of `array::join(txt, ' ')` — so they are evaluated once
	per group, with no row to read. A grouping key is the one row value that
	survives that: it is the group's own value, and it is what the group was
	opened under.

	Each query below reaches a grouping key from such an argument. The
	two-group case is what makes the answer visible in the rows rather than
	only in the count: with no document to read, both groups answer `NONE` and
	join alike, and with one group's document the second group reports the
	first group's separator.

	The nested case pins that the key is found wherever it sits in the
	argument, not only where the whole argument is the key. The literal
	separator and the differently named key are controls — the first is
	untouched by any of this, the second shows the key is reached as the
	group's key rather than by the spelling `g`.

	An argument naming a field that is *not* a grouping key is deliberately
	absent: what it should answer is a separate question from the one these
	queries ask.
"""

[[test.results]]
value = "[{ g: '-', x: 'x-y' }]"

[[test.results]]
value = "[{ g: '-', x: 'x-z' }, { g: ':', x: 'y' }]"

[[test.results]]
value = "[{ g: '-', x: 'x--y' }]"

[[test.results]]
value = "[{ g: '-', x: 'x-y' }]"

[[test.results]]
value = "[{ k: '-', x: 'x-y' }]"
*/

-- The key reaches the separator.
SELECT array::join(w, g) AS x, g FROM [{ g: '-', w: 'x' }, { g: '-', w: 'y' }] GROUP BY g;

-- Two groups, two separators: each group joins with its own key.
SELECT array::join(w, g) AS x, g
	FROM [{ g: '-', w: 'x' }, { g: ':', w: 'y' }, { g: '-', w: 'z' }] GROUP BY g;

-- The key nested inside the argument rather than being the whole of it.
SELECT array::join(w, string::concat(g, g)) AS x, g
	FROM [{ g: '-', w: 'x' }, { g: '-', w: 'y' }] GROUP BY g;

-- A literal separator: the control.
SELECT array::join(w, '-') AS x, g FROM [{ g: '-', w: 'x' }, { g: '-', w: 'y' }] GROUP BY g;

-- The key under another name, reached as the group's key and not as `g`.
SELECT array::join(w, k) AS x, k FROM [{ k: '-', w: 'x' }, { k: '-', w: 'y' }] GROUP BY k;
