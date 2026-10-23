/**
[env]
namespace = true
database = true

[test]
reason = "A statement that creates a record (CREATE with SET or CONTENT, an UPSERT of a new id, a RELATE with SET) or deletes one reads its own document as it stands before the write: NONE, or an object holding only the record's `id` where that id is known before the data clause runs. Either is a row: an index or a method argument in a closure written in it is read against that row, as the same argument written without a closure is, and does not fall back to the receiver the way a frame with no row does. The closure-free twin of each statement is the control. A closure in an UPDATE of an existing record reads the record."

[[test.results]]
value = "[{ id: cn:1, lim: 1 }]"

[[test.results]]
value = "[{ id: cn:2, lim: 2 }]"

[[test.results]]
value = "[[[0, cn:10]]]"

[[test.results]]
value = "[[0, cn:11]]"

[[test.results]]
value = "[[[0, { id: cn:12 }]]]"

[[test.results]]
value = "[[0, { id: cn:13 }]]"

[[test.results]]
value = "[[[0, NONE], [0, NONE]]]"

[[test.results]]
value = "[[0, NONE]]"

[[test.results]]
value = "[[1, 5, 10]]"

[[test.results]]
value = "[[1, 5, 10]]"

[[test.results]]
value = "[[[0, cn:18]]]"

[[test.results]]
value = "[[0, cn:19]]"

[[test.results]]
value = "[[[0, NONE, NONE]]]"

[[test.results]]
value = "[[0, NONE, NONE]]"

[[test.results]]
value = "[[[0, 2], [0, 2]]]"

[[test.results]]
value = "[[0, 2]]"

[[test.results]]
value = '[{ "[1]": [[0, NONE]] }]'

[[test.results]]
value = '[{ "[0]": [0, NONE] }]'

[[test.results]]
value = "[[[0, NONE]]]"

[[test.results]]
value = "[[0, NONE]]"

*/

CREATE cn:1 SET lim = 1;
CREATE cn:2 SET lim = 2;
CREATE cn:10 SET p = [1].map(|$e| [0].concat([id])) RETURN VALUE p;
CREATE cn:11 SET p = [0].concat([id]) RETURN VALUE p;
CREATE cn:12 SET p = [1].map(|$e| [0].concat([$this])) RETURN VALUE p;
CREATE cn:13 SET p = [0].concat([$this]) RETURN VALUE p;
CREATE cn:14 CONTENT { p: [1, 5].map(|$e| [0].concat([lim])) } RETURN VALUE p;
CREATE cn:15 CONTENT { p: [0].concat([lim]) } RETURN VALUE p;
CREATE cn:16 CONTENT { a: [1, 5, 10], r: [1, 5, 10].filter(|$e| $e >= [0].concat([lim]).last()) } RETURN VALUE r;
CREATE cn:17 CONTENT { a: [1, 5, 10], r: [1, 5, 10].filter(|$e| $e >= 0) } RETURN VALUE r;
UPSERT cn:18 SET p = [1].map(|$e| [0].concat([id])) RETURN VALUE p;
UPSERT cn:19 SET p = [0].concat([id]) RETURN VALUE p;
RELATE cn:1->cnrel->cn:2 SET p = [1].map(|$e| [0].concat([id, in])) RETURN VALUE p;
RELATE cn:1->cnrel->cn:2 SET p = [0].concat([id, in]) RETURN VALUE p;
UPDATE cn:2 SET p = [1, 5].map(|$e| [0].concat([lim])) RETURN VALUE p;
UPDATE cn:2 SET p = [0].concat([lim]) RETURN VALUE p;
DELETE cn:2 RETURN [1].map(|$e| [0].concat([lim]));
DELETE cn:1 RETURN [0].concat([lim]);
CREATE cn SET p = [1].map(|$e| [0].concat([$this])) RETURN VALUE p;
CREATE cn SET p = [0].concat([$this]) RETURN VALUE p;
