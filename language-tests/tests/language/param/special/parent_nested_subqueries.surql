/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Test $parent scoping across multiple levels of subquery nesting and graph traversals"

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
value = "[{ id: person:alice, name: 'Alice' }]"

[[test.results]]
value = "[{ id: person:bob, name: 'Bob' }]"

[[test.results]]
value = "[{ id: person:carol, name: 'Carol' }]"

[[test.results]]
value = "[{ id: knows:x, in: person:alice, out: person:bob }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: knows:x, in: person:bob, out: person:carol }]"
skip-record-id-key = true

[[test.results]]
value = "[{ chain: [{ deep: { from_middle: 'Bob' }, from_outer: 'Alice', middle_name: 'Bob' }, { deep: { from_middle: 'Carol' }, from_outer: 'Alice', middle_name: 'Carol' }], name: 'Alice' }, { chain: [{ deep: { from_middle: 'Alice' }, from_outer: 'Bob', middle_name: 'Alice' }, { deep: { from_middle: 'Carol' }, from_outer: 'Bob', middle_name: 'Carol' }], name: 'Bob' }, { chain: [{ deep: { from_middle: 'Alice' }, from_outer: 'Carol', middle_name: 'Alice' }, { deep: { from_middle: 'Bob' }, from_outer: 'Carol', middle_name: 'Bob' }], name: 'Carol' }]"

[[test.results]]
value = "[{ name: 'Alice', sub: [{ edges_to_outer: [], who: 'Alice' }, { edges_to_outer: [], who: 'Bob' }, { edges_to_outer: [], who: 'Carol' }] }, { name: 'Bob', sub: [{ edges_to_outer: [knows:x], who: 'Alice' }, { edges_to_outer: [], who: 'Bob' }, { edges_to_outer: [], who: 'Carol' }] }, { name: 'Carol', sub: [{ edges_to_outer: [], who: 'Alice' }, { edges_to_outer: [knows:x], who: 'Bob' }, { edges_to_outer: [], who: 'Carol' }] }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: person:alice, sub: { edges: [], outer_id: person:alice } }, { id: person:bob, sub: { edges: [knows:x], outer_id: person:bob } }, { id: person:carol, sub: { edges: [], outer_id: person:carol } }]"
skip-record-id-key = true

[[test.results]]
value = "[{ chain: [{ deep: [{ edges_to_mid: [knows:x], mid_name: 'Bob' }], mid: 'Bob' }, { deep: [{ edges_to_mid: [], mid_name: 'Carol' }], mid: 'Carol' }], name: 'Alice' }, { chain: [{ deep: [{ edges_to_mid: [], mid_name: 'Alice' }], mid: 'Alice' }, { deep: [{ edges_to_mid: [], mid_name: 'Carol' }], mid: 'Carol' }], name: 'Bob' }, { chain: [{ deep: [{ edges_to_mid: [], mid_name: 'Alice' }], mid: 'Alice' }, { deep: [{ edges_to_mid: [knows:x], mid_name: 'Bob' }], mid: 'Bob' }], name: 'Carol' }]"
skip-record-id-key = true

*/

DEFINE TABLE OVERWRITE person SCHEMAFULL;
DEFINE FIELD OVERWRITE name ON person TYPE string;

DEFINE TABLE OVERWRITE knows SCHEMAFULL TYPE RELATION FROM person TO person ENFORCED;
DEFINE FIELD OVERWRITE in ON knows TYPE record<person>;
DEFINE FIELD OVERWRITE out ON knows TYPE record<person>;

CREATE person:alice SET name = "Alice";
CREATE person:bob SET name = "Bob";
CREATE person:carol SET name = "Carol";

RELATE person:alice->knows->person:bob;
RELATE person:bob->knows->person:carol;

-- 3-level nesting: outer -> sub1 (table scan) -> sub2 (scalar).
-- sub1's $parent = outer row. sub2's $parent = sub1's current row.
SELECT name,
  (SELECT
    $parent.name AS from_outer,
    name AS middle_name,
    (SELECT $parent.name AS from_middle FROM ONLY true) AS deep
  FROM person WHERE name != $parent.name ORDER BY name) AS chain
FROM person ORDER BY name;

-- Graph WHERE inside a subquery that iterates a table.
-- sub1 iterates person, $parent = outer row.
-- The graph WHERE inside sub1 also sees $parent = outer row.
SELECT name,
  (SELECT
    name AS who,
    ->knows[WHERE out = $parent.id] AS edges_to_outer
  FROM person ORDER BY name) AS sub
FROM person ORDER BY name;

-- Graph traversal at depth 2: outer -> sub1 (FROM ONLY true) -> graph WHERE.
-- $parent inside the graph WHERE must be the outer person row (bound by the
-- outermost ScalarSubquery), not the literal `true` from sub1's FROM source.
SELECT id,
  (SELECT
    $parent.id AS outer_id,
    person:alice->knows[WHERE out = $parent.id] AS edges
  FROM ONLY true) AS sub
FROM person ORDER BY id;

-- 3-level with graph at the deepest level: outer -> sub1 (table) -> sub2 (record).
-- sub2's $parent = sub1's current row.
-- The graph inside sub2 uses $parent.id to find edges targeting the mid person.
SELECT name,
  (SELECT
    name AS mid,
    (SELECT
      $parent.name AS mid_name,
      ->knows[WHERE out = $parent.id] AS edges_to_mid
    FROM person:alice) AS deep
  FROM person WHERE name != $parent.name ORDER BY name) AS chain
FROM person ORDER BY name;
