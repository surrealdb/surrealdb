/**
[env]
imports = ["upgrade/define/index_standard_import.surql"]

[test]
upgrade = true
reason = "Standard (non-unique) index survives upgrade and still returns expected rows when used as the iterator"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { idx_std_org: 'DEFINE INDEX idx_std_org ON idx_std FIELDS org' }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[{ id: idx_std:1, name: 'alice', org: 'surrealdb' }, { id: idx_std:2, name: 'bob', org: 'surrealdb' }]"

[[test.results]]
value = "[{ id: idx_std:3, name: 'carol', org: 'other' }]"

*/
INFO FOR TABLE idx_std;
SELECT * FROM idx_std WITH INDEX idx_std_org WHERE org = 'surrealdb' ORDER BY id;
SELECT * FROM idx_std WITH INDEX idx_std_org WHERE org = 'other' ORDER BY id;
