/**
[env]
imports = ["upgrade/define/index/unique_import.surql"]

[test]
upgrade = true

[[test.results]]
value = "[{ name: 'Tobie' }]"

[[test.results]]
value = "[{ name: 'Jaime' }, { name: 'Tobie' }]"
*/

SELECT name FROM people WITH INDEX idx_people_uniq_name WHERE name = 'Tobie';
SELECT name FROM people WITH INDEX idx_org WHERE org = 'SurrealDB';
