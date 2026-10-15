/**
[env]
timeout = 30000
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
An `UPDATE` or a `DELETE` whose condition no record satisfies never evaluates its data clause,
whichever form the clause takes (an invalid cast, a missing function, a subquery, a scalar given
to `CONTENT`, `MERGE` or `REPLACE`, a patch no operation of which exists), on a named target and on a
table, over a table whose stages after the `WHERE` can be dropped, one that stores an event, one
that derives a field, and one a permission predicate guards. The statements answer and leave the
tables as they did.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: a:1, kind: 'p', s: 'one', tags: ['t'], x: 1 }]"

[[test.results]]
value = "[{ id: a:2, kind: 'p', s: 'two', tags: [], x: 2 }]"

[[test.results]]
value = "[{ id: b:1, x: 1 }]"

[[test.results]]
value = "[{ id: c:1, x: 1, y: 2 }]"

[[test.results]]
value = "[{ id: d:1, x: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: a:1, kind: 'p', s: 'one', tags: ['t'], x: 1 }, { id: a:2, kind: 'p', s: 'two', tags: [], x: 2 }]"

[[test.results]]
value = "[{ id: a:1, kind: 'p', s: 'one', tags: ['t'], x: 1 }]"

[[test.results]]
value = "[{ id: a:1, kind: 'p', s: 'one', tags: ['t'], x: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: b:1, x: 1 }]"

[[test.results]]
error = "The table 'log' does not exist"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: c:1, x: 1, y: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: d:1, x: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Could not cast into `int` using input `'bad'`"

[[test.results]]
value = "[{ id: a:1, kind: 'p', s: 'one', tags: ['t'], x: 1 }, { id: a:2, kind: 'p', s: 'two', tags: [], x: 2 }]"
*/

DEFINE TABLE a SCHEMAFULL CHANGEFEED 1h;
DEFINE FIELD x ON a TYPE int ASSERT $value >= 0;
DEFINE FIELD s ON a TYPE string VALUE string::trim($value);
DEFINE FIELD tags ON a TYPE array<string> DEFAULT [];
DEFINE FIELD kind ON a TYPE string ASSERT $value INSIDE ['p', 'q'] DEFAULT 'p';
DEFINE INDEX a_x ON a FIELDS x;
DEFINE INDEX a_s ON a FIELDS s UNIQUE;
DEFINE TABLE b SCHEMALESS;
DEFINE EVENT b_log ON b WHEN $event = 'UPDATE' THEN (CREATE log SET at = $after.id);
DEFINE TABLE c SCHEMALESS;
DEFINE FIELD y ON c COMPUTED x + 1;
DEFINE TABLE d SCHEMALESS PERMISSIONS FOR select, create FULL FOR update, delete WHERE x > 0;
CREATE a:1 SET x = 1, s = ' one ', tags = ['t'];
CREATE a:2 SET x = 2, s = ' two ';
CREATE b:1 SET x = 1;
CREATE c:1 SET x = 1;
CREATE d:1 SET x = 1;
UPDATE a:1 SET x = <int> 'bad' WHERE false;
UPDATE a SET x = <int> 'bad' WHERE false;
UPDATE ONLY a:1 SET x = <int> 'bad' WHERE false;
UPDATE a:1, a:2 SET x = <int> 'bad' WHERE NONE RETURN BEFORE;
UPDATE a SET x = <int> 'bad' WHERE NULL RETURN AFTER;
UPDATE a:1 SET x = fn::nope() WHERE false;
UPDATE a SET x = fn::nope() WHERE false;
UPDATE a:1 SET x = (SELECT * FROM nope) WHERE false;
UPDATE a SET x = (SELECT * FROM nope) WHERE false;
UPDATE a:1 SET x = throw 'no' WHERE false;
UPDATE a:1 CONTENT 5 WHERE false;
UPDATE a CONTENT 5 WHERE false;
UPDATE a:1 MERGE 5 WHERE false;
UPDATE a MERGE 5 WHERE false;
UPDATE a:1 REPLACE 5 WHERE false;
UPDATE a REPLACE 5 WHERE false;
UPDATE a:1 PATCH [{ op: 'bogus', path: '/x' }] WHERE false;
UPDATE a PATCH [{ op: 'bogus', path: '/x' }] WHERE false;
UPDATE a:1 SET x = -1 WHERE false;
UPDATE a SET s = 5 WHERE false;
UPDATE a:1 SET s = ' two ' WHERE false;
UPDATE a UNSET x WHERE false;
DELETE a:1 WHERE false;
DELETE a WHERE false;
DELETE a:1 WHERE NONE RETURN BEFORE;
DELETE a WHERE NULL RETURN BEFORE;
SELECT * FROM a ORDER BY id;
SELECT * FROM a WHERE s = 'one';
SELECT * FROM a WHERE x = 1;
UPDATE b:1 SET x = <int> 'bad' WHERE false;
UPDATE b SET x = <int> 'bad' WHERE false;
UPDATE b:1 CONTENT 5 WHERE false;
UPDATE b SET x = fn::nope() WHERE false;
DELETE b:1 WHERE false;
DELETE b WHERE false;
SELECT * FROM b;
SELECT * FROM log;
UPDATE c:1 SET x = <int> 'bad' WHERE false;
UPDATE c SET x = <int> 'bad' WHERE false;
UPDATE c:1 SET y = 5 WHERE false;
DELETE c:1 WHERE false;
DELETE c WHERE false;
SELECT * FROM c;
UPDATE d:1 SET x = <int> 'bad' WHERE false;
UPDATE d SET x = <int> 'bad' WHERE false;
DELETE d:1 WHERE false;
DELETE d WHERE false;
SELECT * FROM d;
UPDATE a:1 SET x = <int> 'bad' WHERE false AND true;
UPDATE a SET x = <int> 'bad' WHERE true AND false;
UPDATE a SET x = <int> 'bad' WHERE false OR false;
UPDATE a:1 SET x = <int> 'bad' WHERE true;
SELECT * FROM a ORDER BY id;
