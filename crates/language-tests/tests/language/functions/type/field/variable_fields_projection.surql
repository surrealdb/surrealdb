/**
[test]

[[test.results]]
value = "[{ id: person:test, name: { first: 'Tobie', last: 'Morgan Hitchcock' }, title: 'Mr' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ name: { first: 'Tobie', last: 'Morgan Hitchcock' }, title: 'Mr' }]"

[[test.results]]
value = "[{ names: ['Tobie', 'Morgan Hitchcock'] }]"

[[test.results]]
value = "[['Tobie', 'Morgan Hitchcock']]"

*/

CREATE person:test SET title = 'Mr', name.first = 'Tobie', name.last = 'Morgan Hitchcock';
LET $param = ['name.first', 'name.last'];
SELECT type::fields($param), type::fields(['title']) FROM person;
SELECT VALUE { 'names': type::fields($param) } FROM person;
SELECT VALUE type::fields($param) FROM person;
