/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Test $parent access inside graph traversal WHERE clauses"

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
value = "[{ id: knows:x, in: person:carol, out: person:alice }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: person:alice, matching_edges: [] }, { id: person:bob, matching_edges: [knows:x] }, { id: person:carol, matching_edges: [] }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: person:alice, name: 'Alice', others: [person:bob] }, { id: person:bob, name: 'Bob', others: [person:carol] }, { id: person:carol, name: 'Carol', others: [person:alice] }]"

[[test.results]]
value = "[{ id: person:alice, self_referencing: [] }, { id: person:bob, self_referencing: [] }, { id: person:carol, self_referencing: [] }]"

[[test.results]]
value = "[{ alice_knows_me: false, id: person:alice }, { alice_knows_me: true, id: person:bob }, { alice_knows_me: false, id: person:carol }]"

[[test.results]]
value = "[{ id: person:alice, sub: { edges: [], outer_id: person:alice } }, { id: person:bob, sub: { edges: [], outer_id: person:bob } }, { id: person:carol, sub: { edges: [], outer_id: person:carol } }]"

[[test.results]]
value = "[{ distant_contacts: ['Bob'], id: person:alice, name: 'Alice' }, { distant_contacts: ['Carol'], id: person:bob, name: 'Bob' }, { distant_contacts: ['Alice'], id: person:carol, name: 'Carol' }]"

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
RELATE person:carol->knows->person:alice;

-- $parent.id in forward edge WHERE
SELECT
  id,
  person:alice->knows[WHERE out = $parent.id] AS matching_edges
FROM person ORDER BY id;

-- $parent.name in forward target node WHERE
SELECT
  id,
  name,
  ->knows->(person WHERE name != $parent.name) AS others
FROM person ORDER BY id;

-- $parent.id in reverse edge WHERE
SELECT
  id,
  <-knows[WHERE in = $parent.id] AS self_referencing
FROM person ORDER BY id;

-- Boolean coercion with !! on graph WHERE using $parent (pattern from issue #7083)
SELECT
  id,
  !!(person:alice->knows[WHERE out = $parent.id]) AS alice_knows_me
FROM person ORDER BY id;

-- $parent in graph WHERE inside a subquery (FROM ONLY true).
-- $parent.id in the direct projection = outer row (subquery parent).
-- $parent.id in the graph WHERE = inner row (true), so edges is always empty.
SELECT
  id,
  (SELECT $parent.id AS outer_id, person:alice->knows[WHERE out = $parent.id] AS edges FROM ONLY true) AS sub
FROM person ORDER BY id;

-- $parent.name in edge WHERE combined with further path traversal
SELECT
  id,
  name,
  ->knows[WHERE out != $parent.id]->person.name AS distant_contacts
FROM person ORDER BY id;
