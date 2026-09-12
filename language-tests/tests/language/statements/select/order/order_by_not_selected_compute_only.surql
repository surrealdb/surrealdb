/**
[env]
namespace = true
database = true
planner-strategy = ["compute-only"]

[test]
reason = "The legacy compute pipeline projects each document during iteration, so a sort key outside the projection is gone before the sort runs. It must reject the shape rather than return rows in scan order. The streaming executor has no such limit; see order_by_computed_not_selected.surql."

[[test.results]]
value = "[{ age: 30, id: t:1, name: 'alice' }]"

[[test.results]]
value = "[{ age: 20, id: t:2, name: 'bob' }]"

[[test.results]]
error = "Invalid query: Cannot ORDER BY `age` because it is not in the statement selection. Either add it to the selection, or use a planner strategy other than 'compute-only'."

[[test.results]]
value = "[{ age: 20, id: t:2, name: 'bob' }, { age: 30, id: t:1, name: 'alice' }]"

[[test.results]]
value = "['bob', 'alice']"

[[test.results]]
value = "[{ age: 20, name: 'bob' }, { age: 30, name: 'alice' }]"

[[test.results]]
value = "[{ age: 20, name: 'bob' }, { age: 30, name: 'alice' }]"

[[test.results]]
error = "Invalid query: Cannot ORDER BY `age` because it is not in the statement selection. Either add it to the selection, or use a planner strategy other than 'compute-only'."

[[test.results]]
value = "[{ name: 'bob', years: 20 }, { name: 'alice', years: 30 }]"

[[test.results]]
value = "[{ age: 20, years: 20 }, { age: 30, years: 30 }]"

*/

CREATE t:1 SET name = "alice", age = 30;
CREATE t:2 SET name = "bob", age = 20;

-- Rejected: `age` cannot survive the projection.
SELECT id, name FROM t ORDER BY age ASC;

-- Accepted: `*` carries every field.
SELECT * FROM t ORDER BY age ASC;

-- Accepted: SELECT VALUE defers its projection until after the sort.
SELECT VALUE name FROM t ORDER BY age ASC;

-- Accepted: the sort key is projected.
SELECT name, age FROM t ORDER BY age ASC;

-- Accepted: an alias matching the ordering idiom.
SELECT name, age AS age FROM t ORDER BY age ASC;

-- Rejected: a renamed selector puts only `years` on the row, so the sort
-- key is gone for the same reason.
SELECT name, age AS years FROM t ORDER BY age ASC;

-- Accepted: the name the projected row actually carries.
SELECT name, age AS years FROM t ORDER BY years ASC;

-- Accepted: an unaliased copy alongside the rename keeps the key.
SELECT age, age AS years FROM t ORDER BY age ASC;
