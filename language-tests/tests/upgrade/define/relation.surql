/**
[env]
imports = ["upgrade/define/relation_import.surql"]

[test]
upgrade = true
reason = "RELATION-typed table and the IN/OUT graph edges it contains survive upgrade — schema definition, edge records, and graph traversal all work post-upgrade"

[[test.results]]
match = '''
    string::contains($result.tables.knows, "DEFINE TABLE knows TYPE RELATION")
    && string::contains($result.tables.knows, "IN person")
    && string::contains($result.tables.knows, "OUT person")
'''

[[test.results]]
value = "[{ id: knows:⟨k1⟩, in: person:alice, out: person:bob, since: d'2025-01-01T00:00:00Z' }, { id: knows:⟨k2⟩, in: person:bob, out: person:carol, since: d'2025-02-01T00:00:00Z' }]"

[[test.results]]
value = "[{ friends: [person:bob], id: person:alice }, { friends: [person:carol], id: person:bob }, { friends: [], id: person:carol }]"

*/
INFO FOR DB;
SELECT * FROM knows ORDER BY id;
SELECT id, ->knows->person AS friends FROM person ORDER BY id;
