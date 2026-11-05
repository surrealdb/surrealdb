/**
[env]
namespace = true
database = true

[test]
reason = "ORDER BY sorts the full record before the projection runs, so an ordering field need not appear in the SELECT list. Covers ASC, DESC, the bounded top-k path via LIMIT, an index-backed ordering, and multiple ordering keys."

[[test.results]]
value = "[{ age: 30, id: t:1, name: 'alice' }]"

[[test.results]]
value = "[{ age: 20, id: t:2, name: 'bob' }]"

[[test.results]]
value = "[{ age: 40, id: t:3, name: 'carol' }]"

[[test.results]]
value = "[{ id: t:2, name: 'bob' }, { id: t:1, name: 'alice' }, { id: t:3, name: 'carol' }]"

[[test.results]]
value = "[{ id: t:3, name: 'carol' }, { id: t:1, name: 'alice' }, { id: t:2, name: 'bob' }]"

[[test.results]]
value = "[{ name: 'carol' }, { name: 'alice' }]"

[[test.results]]
value = "[{ name: 'carol' }, { name: 'alice' }, { name: 'bob' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ name: 'bob' }, { name: 'alice' }, { name: 'carol' }]"

[[test.results]]
value = "[{ name: 'carol' }, { name: 'alice' }]"

[[test.results]]
value = "[{ age: 20, name: 'bob' }, { age: 30, name: 'alice' }, { age: 40, name: 'carol' }]"

[[test.results]]
value = "[{ name: 'bob', years: 20 }, { name: 'alice', years: 30 }, { name: 'carol', years: 40 }]"

[[test.results]]
value = "[{ name: 'bob', years: 20 }, { name: 'alice', years: 30 }, { name: 'carol', years: 40 }]"

*/

CREATE t:1 SET name = "alice", age = 30;
CREATE t:2 SET name = "bob", age = 20;
CREATE t:3 SET name = "carol", age = 40;

-- The ordering field is absent from the projection.
SELECT id, name FROM t ORDER BY age ASC;
SELECT id, name FROM t ORDER BY age DESC;

-- Bounded top-k: LIMIT with an unprojected ordering field.
SELECT name FROM t ORDER BY age DESC LIMIT 2;

-- Multiple ordering keys, neither projected.
SELECT name FROM t ORDER BY age DESC, name ASC;

-- An index over the ordering field must produce the same order.
DEFINE INDEX t_age ON t FIELDS age;
SELECT name FROM t ORDER BY age ASC;
SELECT name FROM t ORDER BY age DESC LIMIT 2;

-- A projected ordering field still works.
SELECT name, age FROM t ORDER BY age ASC;

-- A renamed selector: the sort reads the record, so the source name orders
-- the rows as readily as the name they are projected under.
SELECT name, age AS years FROM t ORDER BY age ASC;
SELECT name, age AS years FROM t ORDER BY years ASC;
