/**
[test]

[[test.results]]
value = "[{ id: person:test, name: { first: 'Tobie', last: 'Morgan Hitchcock' }, title: 'Mr' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ name: { first: 'Tobie', last: 'Morgan Hitchcock' } }]"

[[test.results]]
value = "[{ firstname: 'Tobie', lastname: 'Morgan Hitchcock' }]"

[[test.results]]
value = "[['Tobie', 'Morgan Hitchcock']]"

[[test.results]]
value = "[{ first_name: 'Tobie' }]"

*/

CREATE person:test SET title = 'Mr', name.first = 'Tobie', name.last = 'Morgan Hitchcock';
LET $param = 'name.first';
SELECT type::field($param), type::field('name.last') FROM person;
SELECT VALUE { 'firstname': type::field($param), lastname: type::field('name.last') } FROM person;
SELECT VALUE [type::field($param), type::field('name.last')] FROM person;
SELECT type::field($param) AS first_name FROM person;
