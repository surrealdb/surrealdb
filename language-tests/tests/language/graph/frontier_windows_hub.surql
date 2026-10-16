/**
[env]
namespace = true
database = true
timeout = 30000

[test]
reason = "A vertex whose adjacency holds more keys than one read of a window takes is read from the window and then from a cursor that goes on after the last key the window holds: the answers, their order, and the pages a LIMIT and START cut before the lookups run must be the ones the reads one at a time give, for the hub among the rows, and among the vertices a later hop reaches"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: knows:99999, in: person:2, out: person:1 }]"

[[test.results]]
value = "[{ c: 1201, id: person:1 }, { c: 3, id: person:2 }, { c: 3, id: person:3 }, { c: 4, id: person:4 }, { c: 0, id: person:5 }]"

[[test.results]]
value = "[{ c: 3, id: person:2 }, { c: 3, id: person:3 }, { c: 4, id: person:4 }, { c: 0, id: person:5 }, { c: 1, id: person:6 }, { c: 2, id: person:7 }]"

[[test.results]]
value = "[{ c: 4, id: person:1 }, { c: 1206, id: person:2 }, { c: 3, id: person:3 }, { c: 8, id: person:4 }, { c: 0, id: person:5 }, { c: 4, id: person:6 }]"

[[test.results]]
value = "[1206, 5, 1206]"

[[test.results]]
value = "[{ first: [person:4, person:1001, person:1002], id: person:1 }, { first: [person:7, person:8, person:1], id: person:2 }, { first: [person:10, person:11, person:12], id: person:3 }, { first: [person:13, person:14, person:15], id: person:4 }]"

[[test.results]]
value = "[3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]"

[[test.results]]
value = "[[person:2000, person:2001, person:2002, person:2003], NONE]"

[[test.results]]
value = "[{ e: 3, id: person:2 }, { e: 3, id: person:3 }, { e: 4, id: person:4 }]"
*/

DEFINE TABLE person SCHEMALESS;
DEFINE TABLE knows TYPE RELATION FROM person TO person;
FOR $i IN 1..=40 { CREATE type::record('person', $i) SET n = $i; };
FOR $i IN 1..=40 {
	FOR $k IN 0..($i % 5) {
		LET $edge = type::record('knows', $i * 10 + $k);
		RELATE (type::record('person', $i))->$edge->(type::record('person', ($i * 3 + $k) % 40 + 1));
	};
};
FOR $i IN 1..=1200 { CREATE type::record('person', 1000 + $i) SET n = 1000 + $i; };
FOR $i IN 1..=1200 {
	LET $edge = type::record('knows', 100000 + $i);
	RELATE person:1->$edge->(type::record('person', 1000 + $i));
};
RELATE person:2->knows:99999->person:1;

SELECT id, array::len(->knows->person) AS c FROM person LIMIT 5;
SELECT id, array::len(->knows->person) AS c FROM person LIMIT 6 START 1;
SELECT id, array::len(->knows->person->knows->person) AS c FROM person LIMIT 6;
SELECT VALUE array::len(->knows->person->knows->person) FROM person:2, person:7, person:2;
SELECT id, ->knows->person[0..3] AS first FROM person LIMIT 4;
SELECT VALUE array::len(<-knows<-person) FROM person:1..=12;
SELECT VALUE ->knows->person[1000..1004] FROM person:1, person:2;
SELECT id, array::len(->knows) AS e FROM person ORDER BY id LIMIT 3 START 1;
