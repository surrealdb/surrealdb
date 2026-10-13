/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Plan-time folding of row-independent record-idiom traversals ($rec.field) lets index analysis match them; the fold bails (table scan, same results) for tables with computed fields"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: org:acme, label: 'ACME' }]"

[[test.results]]
value = "[{ id: person:alice, org: org:acme, profile: { tier: 'gold' } }]"

[[test.results]]
value = "[{ id: ticket:t1, orgLabel: 'ACME', owner: person:alice, region: 'eu', tier: 'gold' }]"

[[test.results]]
value = "[{ id: ticket:t2, orgLabel: 'Other', owner: person:alice, region: 'us', tier: 'bronze' }]"

[[test.results]]
value = "[{ id: ticket:t3, orgLabel: 'ACME', owner: person:bob, region: 'eu', tier: 'gold' }]"

[[test.results]]
value = "[{ id: ticket:t4, region: 'eu', shard: zone:{ n: 1, region: 'eu' } }]"

[[test.results]]
value = "[{ id: ticket:t5, tier: 'pro' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { projections: 'id' }, children: [{ attributes: { access: '= person:alice', direction: 'Forward', index: 'idx_owner' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"

[[test.results]]
value = "[{ id: ticket:t1 }, { id: ticket:t2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { access: "= 'gold'", direction: 'Forward', index: 'idx_tier' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[{ id: ticket:t1 }, { id: ticket:t3 }]"

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { access: "= 'ACME'", direction: 'Forward', index: 'idx_org_label' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[{ id: ticket:t1 }, { id: ticket:t3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { access: "= 'eu'", direction: 'Forward', index: 'idx_region' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[{ id: ticket:t1 }, { id: ticket:t3 }, { id: ticket:t4 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: ticket:t4 }, { id: ticket:t5 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: account:a, plan: 'pro', score: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { projections: 'id' }, children: [{ attributes: { direction: 'Forward', pre_decode_filter: 'no (unsupported predicate)', predicate: 'tier = (account:a).plan', table: 'ticket' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }"

[[test.results]]
value = "[{ id: ticket:t5 }]"
*/

DEFINE INDEX idx_owner ON TABLE ticket COLUMNS owner;
DEFINE INDEX idx_tier ON TABLE ticket COLUMNS tier;
DEFINE INDEX idx_org_label ON TABLE ticket COLUMNS orgLabel;
DEFINE INDEX idx_region ON TABLE ticket COLUMNS region;

CREATE org:acme SET label = 'ACME';
CREATE person:alice SET org = org:acme, profile = { tier: 'gold' };
CREATE ticket:t1 SET owner = person:alice, tier = 'gold', orgLabel = 'ACME', region = 'eu';
CREATE ticket:t2 SET owner = person:alice, tier = 'bronze', orgLabel = 'Other', region = 'us';
CREATE ticket:t3 SET owner = person:bob, tier = 'gold', orgLabel = 'ACME', region = 'eu';
CREATE ticket:t4 SET shard = zone:{ region: 'eu', n: 1 }, region = 'eu';
CREATE ticket:t5 SET tier = 'pro';

LET $t1 = ticket:t1;

-- Single-hop fold: $t1.owner resolves to person:alice at plan time
SELECT id FROM ticket WHERE owner = $t1.owner ORDER BY id EXPLAIN;
SELECT id FROM ticket WHERE owner = $t1.owner ORDER BY id;

-- Nested object path: $p.profile.tier resolves through the record's object field
LET $p = person:alice;
SELECT id FROM ticket WHERE tier = $p.profile.tier ORDER BY id EXPLAIN;
SELECT id FROM ticket WHERE tier = $p.profile.tier ORDER BY id;

-- Chained record links: $t1.owner.org.label fetches person:alice then org:acme
SELECT id FROM ticket WHERE orgLabel = $t1.owner.org.label ORDER BY id EXPLAIN;
SELECT id FROM ticket WHERE orgLabel = $t1.owner.org.label ORDER BY id;

-- Object record-id key component: $t4.shard.region resolves from the id key
-- itself (table `zone` has no records; a fetch would yield NONE instead of 'eu')
LET $t4 = ticket:t4;
SELECT id FROM ticket WHERE region = $t4.shard.region ORDER BY id EXPLAIN;
SELECT id FROM ticket WHERE region = $t4.shard.region ORDER BY id;

-- Missing record: field access yields NONE, matching runtime semantics
LET $ghost = ticket:nope;
SELECT id FROM ticket WHERE owner = $ghost.owner ORDER BY id;

-- Computed-field bail: the runtime dereference evaluates computed fields,
-- which the plan-time fold cannot replicate, so the idiom stays unfolded
-- (table scan) and results are produced by per-row evaluation
DEFINE FIELD score ON TABLE account COMPUTED 1 + 1;
CREATE account:a SET plan = 'pro';
LET $a = account:a;
SELECT id FROM ticket WHERE tier = $a.plan ORDER BY id EXPLAIN;
SELECT id FROM ticket WHERE tier = $a.plan ORDER BY id;
