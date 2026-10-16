/**
[test]
reason = """
	A collection of record links read through FETCH, `.*` and a field over an
	array reads its records one position at a time in input order, however many
	links the collection holds: the results, a missing record's NONE, the
	error of an undefined table at its position, a computed field of a target,
	a record read from inside its own computed field and a lightweight relation
	record all read as they do for a single link.
"""

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ first: 1199, last: 0, len: 1200, ns: 1200 }]"

[[test.results]]
value = "[{ fourth: 1196, len: 1200, named: '99' }]"

[[test.results]]
value = "[{ head: [1199, 1198, 1197, 1196], len: 1200 }]"

[[test.results]]
value = "[{ id: holder:mix, refs: [{ id: person:1, n: 1, name: '1' }, 'text', { id: person:1, n: 1, name: '1' }, [person:2, person:3, person:2], { a: person:2 }, 5, { id: person:2, n: 2, name: '2' }, NONE, { id: person:3, n: 3, name: '3' }] }]"

[[test.results]]
value = "[{ id: holder:mix, refs: [{ id: person:1, n: 1, name: '1' }, 'text', { id: person:1, n: 1, name: '1' }, [{ id: person:2, n: 2, name: '2' }, { id: person:3, n: 3, name: '3' }, { id: person:2, n: 2, name: '2' }], { a: person:2 }, 5, { id: person:2, n: 2, name: '2' }, NONE, { id: person:3, n: 3, name: '3' }] }]"

[[test.results]]
value = "[{ refs: [{ id: person:1, n: 1, name: '1' }, 'text', { id: person:1, n: 1, name: '1' }, [person:2, person:3, person:2], { a: person:2 }, 5, { id: person:2, n: 2, name: '2' }, NONE, { id: person:3, n: 3, name: '3' }] }]"

[[test.results]]
value = "[{ refs: { n: [1, NONE, 1, [2, 3, 2], NONE, NONE, 2, NONE, 3] } }]"

[[test.results]]
value = "[{ id: holder:miss, refs: [{ id: person:1, n: 1, name: '1' }, NONE, { id: person:2, n: 2, name: '2' }, NONE, { id: person:4, n: 4, name: '4' }] }]"

[[test.results]]
value = "[{ refs: { n: [1, NONE, 2, NONE, 4] } }]"

[[test.results]]
error = "The table 'nosuchtable' does not exist"

[[test.results]]
error = "The table 'nosuchtable' does not exist"

[[test.results]]
error = "The table 'nosuchtable' does not exist"

[[test.results]]
value = "[{ id: holder:set, refs: {4, person:1, person:2, person:3} }]"

[[test.results]]
value = "[{ refs: {4, { id: person:1, n: 1, name: '1' }, { id: person:2, n: 2, name: '2' }, { id: person:3, n: 3, name: '3' }} }]"

[[test.results]]
value = "[{ refs: { n: {NONE, 1, 2, 3} } }]"

[[test.results]]
value = "[{ id: holder:team, refs: [{ id: team:a, members: [person:3, person:1, person:2, person:1], names: ['3', '1', '2', '1'], size: 4 }, { id: team:a, members: [person:3, person:1, person:2, person:1], names: ['3', '1', '2', '1'], size: 4 }, { id: person:2, n: 2, name: '2' }] }]"

[[test.results]]
value = "[{ refs: { names: [['3', '1', '2', '1'], ['3', '1', '2', '1'], NONE], size: [4, 4, NONE] } }]"

[[test.results]]
value = "[{ id: node:1, kin: [node:1, node:2, person:1], kin_n: [10, 20, 1], n: 10 }, { id: node:2, kin: [], kin_n: [], n: 20 }]"

[[test.results]]
value = "[{ id: holder:edges, refs: [{ id: knows:[person:1, person:2], in: person:1, out: person:2 }, { id: person:1, n: 1, name: '1' }, { id: knows:[person:2, person:3], in: person:2, out: person:3 }, { id: person:2, n: 2, name: '2' }] }]"

[[test.results]]
value = "[{ refs: [{ id: knows:[person:1, person:2], in: person:1, out: person:2 }, { id: person:1, n: 1, name: '1' }, { id: knows:[person:2, person:3], in: person:2, out: person:3 }, { id: person:2, n: 2, name: '2' }] }]"

[[test.results]]
value = "[{ n: 3 }, { n: 1 }, { n: 9 }, { n: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ n: 3 }, { n: 1 }, { n: 2 }, { n: 1 }]"
*/

{
	FOR $i IN 0..1200 {
		CREATE type::record('person', $i) SET n = $i, name = <string>$i RETURN NONE;
	};
	CREATE holder:big SET refs = array::reverse((SELECT VALUE id FROM person)) RETURN NONE;
	CREATE holder:mix SET refs = [person:1, 'text', person:1, [person:2, person:3, person:2], { a: person:2 }, 5, person:2, NONE, person:3] RETURN NONE;
	CREATE holder:miss SET refs = [person:1, person:5000, person:2, person:6000, person:4] RETURN NONE;
	CREATE holder:bad SET refs = [person:1, person:2, nosuchtable:1, person:4] RETURN NONE;
	CREATE holder:set SET refs = <set>[person:1, person:2, person:3, 4] RETURN NONE;

	DEFINE TABLE team SCHEMALESS;
	DEFINE FIELD names ON team COMPUTED members.name;
	DEFINE FIELD size ON team COMPUTED array::len(members);
	CREATE team:a SET members = [person:3, person:1, person:2, person:1] RETURN NONE;
	CREATE holder:team SET refs = [team:a, team:a, person:2] RETURN NONE;

	DEFINE TABLE node SCHEMALESS;
	DEFINE FIELD kin_n ON node COMPUTED kin.n;
	CREATE node:1 SET n = 10, kin = [node:1, node:2, person:1] RETURN NONE;
	CREATE node:2 SET n = 20, kin = [] RETURN NONE;

	DEFINE TABLE knows TYPE RELATION IN person OUT person LIGHTWEIGHT;
	LET $first = (RELATE person:1->knows->person:2 RETURN VALUE id);
	LET $second = (RELATE person:2->knows->person:3 RETURN VALUE id);
	CREATE holder:edges SET refs = [$first[0], person:1, $second[0], person:2] RETURN NONE;

	RETURN 'OK';
};

SELECT array::len(refs) AS len, refs[0].n AS first, refs[1199].n AS last, array::len(refs.n) AS ns FROM holder:big FETCH refs;
SELECT array::len(refs.*) AS len, refs[3].n AS fourth, refs[1100].name AS named FROM holder:big;
SELECT array::slice(refs.n, 0, 4) AS head, array::len(refs.name) AS len FROM holder:big;
SELECT * FROM holder:mix FETCH refs;
SELECT * FROM holder:mix FETCH refs.*;
SELECT refs.* FROM holder:mix;
SELECT refs.n FROM holder:mix;
SELECT * FROM holder:miss FETCH refs;
SELECT refs.n FROM holder:miss;
SELECT * FROM holder:bad FETCH refs;
SELECT refs.* FROM holder:bad;
SELECT refs.n FROM holder:bad;
SELECT * FROM holder:set FETCH refs;
SELECT refs.* FROM holder:set;
SELECT refs.n FROM holder:set;
SELECT * FROM holder:team FETCH refs;
SELECT refs.names, refs.size FROM holder:team;
SELECT * FROM node;
SELECT * FROM holder:edges FETCH refs;
SELECT refs.* FROM holder:edges;
SELECT n FROM [person:3, person:1, person:9, person:1];
LET $ids = [person:3, person:1, person:2, person:1];
SELECT n FROM $ids;
