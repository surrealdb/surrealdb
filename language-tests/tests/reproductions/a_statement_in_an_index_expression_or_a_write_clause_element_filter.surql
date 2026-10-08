/**
[env]
namespace = true
database = true

[test]
reason = "A statement in an index expression, or in the element filter of a field clause, an index expression, an event or a table or field permission, is evaluated against the record the write images, and every write to the table runs: an index over a subquery is maintained by CREATE, UPDATE and DELETE, and a COUNT index's subquery guard is judged."

[[test.results]]
value = "[{ id: cat:1, name: 'A' }]"

[[test.results]]
error = "Expected `field name` but found `['A']`"

[[test.results]]
value = "[{ id: i5:1 }]"

[[test.results]]
value = "[{ id: i5:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: i6:1, v: [1] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: i7:1, tags: ['A', 'B'] }]"

[[test.results]]
value = "[{ id: i7:1, tags: ['B'] }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: i8:1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ id: i9:1, tags: ['A', 'B'] }]"

[[test.results]]
value = "[['A']]"

[[test.results]]
value = "[{ id: i9:2, tags: ['A'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: ['A'], id: i10:1, tags: ['A', 'B'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: j1:1, tags: ['A', 'B'] }]"

[[test.results]]
value = "[{ v: ['A'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ g: 1, id: j2:1, tags: ['A'] }]"

[[test.results]]
value = "[{ g: 2, id: j2:1, tags: ['A', 'B'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ h: ['B'], id: j3:1 }]"

[[test.results]]
error = "Found ['A'] for field `h`, with record `j3:2`, but field must conform to: $value[WHERE $this INSIDE (SELECT VALUE name FROM cat)] = []"

*/
CREATE cat:1 SET name = 'A';
DEFINE INDEX s ON i5 FIELDS (SELECT VALUE name FROM cat);
CREATE i5:1;
SELECT * FROM i5 WHERE (SELECT VALUE name FROM cat) = ['A'];
DEFINE INDEX s6 ON i6 FIELDS v.map(|$x| (SELECT VALUE name FROM cat));
CREATE i6:1 SET v = [1];
DEFINE INDEX s7 ON i7 FIELDS tags[WHERE $this IN (SELECT VALUE name FROM cat)];
CREATE i7:1 SET tags = ['A', 'B'];
UPDATE i7:1 SET tags = ['B'];
DELETE i7:1;
DEFINE INDEX c ON i8 COUNT WHERE (SELECT VALUE name FROM cat)[0] = 'A';
CREATE i8:1;
SELECT count() FROM i8 GROUP ALL;
CREATE i9:1 SET tags = ['A', 'B'];
SELECT VALUE tags[WHERE $this IN (SELECT VALUE name FROM cat)] FROM i9;
CREATE i9:2 SET t = tags[WHERE $this IN (SELECT VALUE name FROM cat)], tags = ['A'];
DEFINE FIELD f ON i10 VALUE tags[WHERE $this IN (SELECT VALUE name FROM cat)];
CREATE i10:1 SET tags = ['A', 'B'];
DEFINE EVENT e ON j1 WHEN tags[WHERE $this IN (SELECT VALUE name FROM cat)] != [] THEN { CREATE jlog SET v = $after.tags[WHERE $this IN (SELECT VALUE name FROM cat)] };
CREATE j1:1 SET tags = ['A', 'B'];
SELECT v FROM jlog;
DEFINE TABLE j2 PERMISSIONS FOR create, update, select WHERE tags[WHERE $this IN (SELECT VALUE name FROM cat)] != [];
DEFINE FIELD g ON j2 VALUE tags.len() PERMISSIONS FOR update WHERE $value[WHERE $this IN (SELECT VALUE name FROM cat)] = [];
DEFINE USER u ON DATABASE PASSWORD 'p' ROLES EDITOR;
CREATE j2:1 SET tags = ['A'];
UPDATE j2:1 SET tags = ['A', 'B'];
DEFINE FIELD h ON j3 ASSERT $value[WHERE $this IN (SELECT VALUE name FROM cat)] = [];
CREATE j3:1 SET h = ['B'];
CREATE j3:2 SET h = ['A'];
