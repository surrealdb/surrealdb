/**
[test]
reason = "Test recursive graph traversal with depth limits"

[[test.results]]
value = "[{ id: person:tobie, name: 'Tobie' }, { id: person:jaime, name: 'Jaime' }, { id: person:micha, name: 'Micha' }, { id: person:john, name: 'John' }, { id: person:mary, name: 'Mary' }, { id: person:tim, name: 'Tim' }]"

[[test.results]]
value = "[{ id: knows:1, in: person:tobie, out: person:jaime }, { id: knows:2, in: person:tobie, out: person:micha }, { id: knows:3, in: person:micha, out: person:john }, { id: knows:4, in: person:jaime, out: person:mary }, { id: knows:5, in: person:mary, out: person:tim }]"

[[test.results]]
value = "[{ name: 'Jaime', names_1sts: ['Mary'] }, { name: 'John', names_1sts: [] }, { name: 'Mary', names_1sts: ['Tim'] }, { name: 'Micha', names_1sts: ['John'] }, { name: 'Tim', names_1sts: [] }, { name: 'Tobie', names_1sts: ['Jaime', 'Micha'] }]"

[[test.results]]
value = "[{ name: 'Jaime', names_2nds: ['Tim'] }, { name: 'John', names_2nds: [] }, { name: 'Mary', names_2nds: [] }, { name: 'Micha', names_2nds: [] }, { name: 'Tim', names_2nds: [] }, { name: 'Tobie', names_2nds: ['Mary', 'John'] }]"

[[test.results]]
value = "[{ name: 'Jaime', names_3rds: [] }, { name: 'John', names_3rds: [] }, { name: 'Mary', names_3rds: [] }, { name: 'Micha', names_3rds: [] }, { name: 'Tim', names_3rds: [] }, { name: 'Tobie', names_3rds: ['Tim'] }]"

[[test.results]]
value = "[{ knows: [{ knows: [{ knows: [], name: 'Tim' }], name: 'Mary' }], name: 'Jaime' }, { knows: [], name: 'John' }, { knows: [{ knows: [], name: 'Tim' }], name: 'Mary' }, { knows: [{ knows: [], name: 'John' }], name: 'Micha' }, { knows: [], name: 'Tim' }, { knows: [{ knows: [{ knows: [{ knows: [], name: 'Tim' }], name: 'Mary' }], name: 'Jaime' }, { knows: [{ knows: [], name: 'John' }], name: 'Micha' }], name: 'Tobie' }]"

[env]
timeout = 5000
*/

INSERT INTO person [
	{ id: person:tobie, name: 'Tobie' },
	{ id: person:jaime, name: 'Jaime' },
	{ id: person:micha, name: 'Micha' },
	{ id: person:john, name: 'John' },
	{ id: person:mary, name: 'Mary' },
	{ id: person:tim, name: 'Tim' },
];

INSERT RELATION INTO knows [
	{ id: knows:1, in: person:tobie, out: person:jaime },
	{ id: knows:2, in: person:tobie, out: person:micha },
	{ id: knows:3, in: person:micha, out: person:john },
	{ id: knows:4, in: person:jaime, out: person:mary },
	{ id: knows:5, in: person:mary, out: person:tim },
];

SELECT name, @{1}(->knows->person).name AS names_1sts FROM person;
SELECT name, @{2}(->knows->person).name AS names_2nds FROM person;
SELECT name, @{3}(->knows->person).name AS names_3rds FROM person;

SELECT VALUE @{..}.{ name, knows: ->knows->person.@ } FROM person;
