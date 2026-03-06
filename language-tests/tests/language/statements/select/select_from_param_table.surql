/**
[test]
reason = "Test SELECT from a parameterized table name"

[[test.results]]
value = "[{ id: person:1, name: 'Alice' }]"

[[test.results]]
value = "[{ id: person:2, name: 'Bob' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:1, name: 'Alice' }, { id: person:2, name: 'Bob' }]"

*/

CREATE person:1 SET name = 'Alice';
CREATE person:2 SET name = 'Bob';

LET $tbl = type::table('person');
SELECT * FROM $tbl ORDER BY name;
