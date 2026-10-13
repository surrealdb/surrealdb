/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "A record-idiom traversal through a missing record folds to NONE (as the runtime walk yields), and the plan pins whether the folded `= NONE` predicate is index-eligible — a rows-only assertion cannot distinguish a NONE fold from a table-scan bail"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: ticket:t1, owner: person:alice }]"

[[test.results]]
value = "[{ id: ticket:t2 }]"

[[test.results]]
value = "[{ id: ticket:t3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { projections: 'id' }, children: [{ attributes: { access: '= NONE', direction: 'Forward', index: 'idx_owner' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"

[[test.results]]
value = "[{ id: ticket:t2 }, { id: ticket:t3 }]"
*/

DEFINE INDEX idx_owner ON TABLE ticket COLUMNS owner;
CREATE ticket:t1 SET owner = person:alice;
CREATE ticket:t2;
CREATE ticket:t3;

LET $ghost = ticket:nope;

SELECT id FROM ticket WHERE owner = $ghost.owner EXPLAIN;
SELECT id FROM ticket WHERE owner = $ghost.owner ORDER BY id;
