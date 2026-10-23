/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A record's content is an object. A write whose resulting content is not an object is refused with the CONTENT clause's error, whichever clause or PATCH operation produced it, and leaves the record as it was. A MERGE or CONTENT value that is not an object is refused by its own clause, not by the record id check that follows it."

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "[{ a: 2, id: t:2 }]"

[[test.results]]
value = "[{ a: 3, id: t:3 }]"

[[test.results]]
error = "Cannot use 5 in a CONTENT clause"

[[test.results]]
error = "Cannot use 1 in a CONTENT clause"

[[test.results]]
error = "Cannot use [1, 2] in a CONTENT clause"

[[test.results]]
error = "Cannot use 3 in a CONTENT clause"

[[test.results]]
error = "Cannot use 3 in a CONTENT clause"

[[test.results]]
error = "Cannot use [1] in a MERGE clause"

[[test.results]]
error = "Cannot use [1] in a CONTENT clause"

[[test.results]]
error = "Cannot use [1] in a MERGE clause"

[[test.results]]
error = "Cannot use 5 in a CONTENT clause"

[[test.results]]
error = "Cannot use [1] in a MERGE clause"

[[test.results]]
error = "Cannot use [1] in a CONTENT clause"

[[test.results]]
error = "Cannot use 5 in a CONTENT clause"

[[test.results]]
value = "[{ a: 3, id: t:3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:3 }]"

[[test.results]]
value = "[{ id: t:3 }]"

[[test.results]]
value = "[{ id: t:3 }]"

[[test.results]]
value = "[{ id: t:3 }]"
*/

-- Three records with object content.
CREATE t:1 SET a = 1;
CREATE t:2 SET a = 2;
CREATE t:3 SET a = 3;

-- A PATCH operation that replaces the root with a scalar is refused.
UPDATE t:3 PATCH [{op: 'replace', path: '', value: 5}];
UPDATE t:3 PATCH [{op: 'add', path: '', value: 1}];
UPDATE t:3 PATCH [{op: 'replace', path: '', value: [1, 2]}];
UPDATE t:3 PATCH [{op: 'move', from: '/a', path: ''}];
UPDATE t:3 PATCH [{op: 'copy', from: '/a', path: ''}];

-- A MERGE or CONTENT value that is not an object is refused by its clause.
UPDATE t:3 MERGE [1];
UPDATE t:3 CONTENT [1];
UPSERT t:3 MERGE [1];
UPSERT t:3 CONTENT 5;
UPSERT t:4 MERGE [1];
UPSERT t:4 CONTENT [1];
UPDATE t:3 REPLACE 5;

-- The refused writes left every record as it was.
SELECT * FROM t:3;
SELECT * FROM t:4;

-- A root replaced by null or NONE becomes the record of its id alone.
UPDATE t:3 PATCH [{op: 'replace', path: '', value: NONE}];
SELECT * FROM t:3;
UPDATE t:3 PATCH [{op: 'replace', path: '', value: null}];
SELECT * FROM t:3;
