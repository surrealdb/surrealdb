/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: people:tobie, name: 'Tobie', org: 'SurrealDB' }]"

[[test.results]]
value = "[{ id: people:jaime, name: 'Jaime', org: 'SurrealDB' }]"

*/
DEFINE INDEX idx_people_uniq_name ON TABLE people COLUMNS name UNIQUE;
DEFINE INDEX idx_org ON TABLE people COLUMNS org;
CREATE people:tobie SET name = 'Tobie', org='SurrealDB';
CREATE people:jaime SET name = 'Jaime', org='SurrealDB';
