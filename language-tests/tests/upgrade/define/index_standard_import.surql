/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: idx_std:1, name: 'alice', org: 'surrealdb' }]"

[[test.results]]
value = "[{ id: idx_std:2, name: 'bob', org: 'surrealdb' }]"

[[test.results]]
value = "[{ id: idx_std:3, name: 'carol', org: 'other' }]"

*/
DEFINE INDEX idx_std_org ON TABLE idx_std COLUMNS org;
CREATE idx_std:1 SET name = 'alice', org = 'surrealdb';
CREATE idx_std:2 SET name = 'bob', org = 'surrealdb';
CREATE idx_std:3 SET name = 'carol', org = 'other';
