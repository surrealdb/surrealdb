/**
[test]
reason = "Setup: create table and data for viewer write denial test"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:1, name: 'alpha' }]"

[[test.results]]
value = "[{ id: item:2, name: 'beta' }]"

*/
DEFINE TABLE item;
DEFINE TABLE rel TYPE RELATION;
CREATE item:1 SET name = 'alpha';
CREATE item:2 SET name = 'beta';
