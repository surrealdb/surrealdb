/**
[env]
namespace = true
database = true

[test]
reason = "A statement in the element filter of a field clause, an event or a table or field permission, or in the guard of a COUNT index, is evaluated against the record the write images, and every write to the table runs: a COUNT index's subquery guard is judged. A statement in a column of an index is refused where the index is defined, because a column reading another record cannot be maintained from the record it indexes (register #28, region_constraints/index_other_record.surql)."

[[test.results]]
value = "[{ id: cat:1, name: 'A' }]"

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
