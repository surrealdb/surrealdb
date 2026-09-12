/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A method's arguments evaluate against the record the idiom started from, not against the receiver, on a stored row, a value-literal row and a COMPUTED field body"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[['b']]"

[[test.results]]
value = "[['x', 'y']]"

[[test.results]]
value = "[['a!', 'b!', 'c!', 'd!']]"

[[test.results]]
value = "[[1, 2]]"

[[test.results]]
value = "[{ a: 1, c: 3, extra: { b: 2 } }]"

[[test.results]]
value = "[['b']]"

[[test.results]]
value = "[['x', 'y']]"

[[test.results]]
value = "[['b']]"

*/

{
	CREATE person:1 SET
		tags = ['a', 'b', 'c', 'd'],
		offset = 1,
		len = 2,
		text = 'x-y',
		sep = '-',
		suffix = '!',
		stats = { count: 10, items: [1, 2, 3] },
		count = 2,
		cfg = { a: 1, extra: { b: 2 } },
		extra = { c: 3 };
	DEFINE FIELD sliced ON person COMPUTED tags.slice(offset, len);
	RETURN 'OK';
};

-- Stored row: `offset` and `len` are fields of the record, not of `tags`;
-- `slice(start, end)` is end-exclusive, so `slice(1, 2)` is `['b']`.
SELECT VALUE tags.slice(offset, len) FROM person:1;
SELECT VALUE text.split(sep) FROM person:1;

-- The body of a closure argument reads the record too.
SELECT VALUE tags.map(|$t| $t + suffix) FROM person:1;

-- The receiver is only the chain's position: `count` is the record's field,
-- not an element of `stats.items`.
SELECT VALUE stats.items.slice(0, count) FROM person:1;

-- When the receiver and the record share a field name, the argument is the
-- record's. Were it the receiver's, this would yield `{ a: 1, b: 2, extra: { b: 2 } }`.
SELECT VALUE cfg.extend(extra) FROM person:1;

-- A row that is a value literal rather than a stored record.
SELECT VALUE tags.slice(offset, len) FROM [{ tags: ['a', 'b', 'c', 'd'], offset: 1, len: 2 }];
SELECT VALUE text.split(sep) FROM [{ text: 'x-y', sep: '-' }];

-- A COMPUTED field body runs against the record it is computed for.
SELECT VALUE sliced FROM person:1;
