/**
[env]

[test]
reason = "Test EXPLAIN and EXPLAIN FULL with multiple FROM tables"

[[test.results]]
value = "[{ id: person:tobie, name: 'Tobie' }]"

[[test.results]]
value = "[{ id: person:jaime, name: 'Jaime' }]"

[[test.results]]
value = "[{ id: software:surreal, name: 'SurrealDB' }]"

*/
CREATE person:tobie SET name = 'Tobie';
CREATE person:jaime SET name = 'Jaime';
CREATE software:surreal SET name = 'SurrealDB';
