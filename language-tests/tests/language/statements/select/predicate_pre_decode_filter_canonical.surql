/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = "Wire-fast canonical-byte-eq path covers more than tag-only literals: Datetime, Uuid, Duration literals are pre-encoded into LiteralWire::Canonical so WHERE created = d'...' / id = u'...' / dur = 5s memcmp against the leaf's wire bytes — no decode."

[[test.results]]
value = "{ at: d'2024-05-21T00:00:00Z', dur: 5s, id: pfcanon:1, uid: u'00000000-0000-0000-0000-0000000000a1' }"

[[test.results]]
value = "{ at: d'2024-05-22T00:00:00Z', dur: 10s, id: pfcanon:2, uid: u'00000000-0000-0000-0000-0000000000a2' }"

# WHERE at = d'2024-05-21...'
[[test.results]]
value = "[{ id: pfcanon:1 }]"

# WHERE uid = u'0...a2'
[[test.results]]
value = "[{ id: pfcanon:2 }]"

# WHERE dur = 5s
[[test.results]]
value = "[{ id: pfcanon:1 }]"

# WHERE at != d'2024-05-21...'
[[test.results]]
value = "[{ id: pfcanon:2 }]"

# EXPLAIN: all three predicate shapes report pre_decode_filter: yes
[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: pfcanon, direction: Forward, predicate: at = d'2024-05-21T00:00:00Z', pre_decode_filter: yes]
"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: pfcanon, direction: Forward, predicate: uid = u'00000000-0000-0000-0000-0000000000a2', pre_decode_filter: yes]
"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: pfcanon, direction: Forward, predicate: dur = 5s, pre_decode_filter: yes]
"'''

*/

CREATE ONLY pfcanon:1 SET at = d'2024-05-21T00:00:00Z', uid = u'00000000-0000-0000-0000-0000000000a1', dur = 5s;
CREATE ONLY pfcanon:2 SET at = d'2024-05-22T00:00:00Z', uid = u'00000000-0000-0000-0000-0000000000a2', dur = 10s;

SELECT id FROM pfcanon WHERE at = d'2024-05-21T00:00:00Z';
SELECT id FROM pfcanon WHERE uid = u'00000000-0000-0000-0000-0000000000a2';
SELECT id FROM pfcanon WHERE dur = 5s;
SELECT id FROM pfcanon WHERE at != d'2024-05-21T00:00:00Z';

EXPLAIN SELECT id FROM pfcanon WITH NOINDEX WHERE at = d'2024-05-21T00:00:00Z';
EXPLAIN SELECT id FROM pfcanon WITH NOINDEX WHERE uid = u'00000000-0000-0000-0000-0000000000a2';
EXPLAIN SELECT id FROM pfcanon WITH NOINDEX WHERE dur = 5s;
