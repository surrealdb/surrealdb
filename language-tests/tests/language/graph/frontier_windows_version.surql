/**
[env]
backend = ["surrealkv"]
versioned = true
timeout = 5000

[test]
reason = "A traversal at a VERSION, and a nested read at a VERSION inside rows read at the present, reads the adjacency that was current at its stamp and never the keys another read of the same statement took together: each read answers as of its own stamp"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: [person:2], id: person:1 }, { f: [person:3], id: person:2 }, { f: [person:4], id: person:3 }, { f: [person:5], id: person:4 }, { f: [person:6], id: person:5 }, { f: [person:7], id: person:6 }, { f: [person:8], id: person:7 }, { f: [person:9], id: person:8 }, { f: [person:10], id: person:9 }, { f: [person:11], id: person:10 }, { f: [person:12], id: person:11 }, { f: [person:1], id: person:12 }]"

[[test.results]]
value = "[{ f: [person:5], id: person:1 }, { f: [person:6], id: person:2 }, { f: [person:7], id: person:3 }, { f: [person:5, person:8], id: person:4 }, { f: [person:6, person:9], id: person:5 }, { f: [person:7, person:10], id: person:6 }, { f: [person:8, person:11], id: person:7 }, { f: [person:9, person:12], id: person:8 }, { f: [person:10, person:1], id: person:9 }, { f: [person:11, person:2], id: person:10 }, { f: [person:12, person:3], id: person:11 }, { f: [person:1, person:4], id: person:12 }]"

[[test.results]]
value = "[{ f: [person:5, person:7], id: person:1 }, { f: [person:6, person:8], id: person:2 }, { f: [person:7, person:9], id: person:3 }, { f: [person:5, person:8, person:10], id: person:4 }, { f: [person:6, person:9, person:11], id: person:5 }, { f: [person:7, person:10, person:12], id: person:6 }, { f: [person:8, person:11, person:1], id: person:7 }, { f: [person:9, person:12, person:2], id: person:8 }, { f: [person:10, person:1, person:3], id: person:9 }, { f: [person:11, person:2, person:4], id: person:10 }, { f: [person:12, person:3, person:5], id: person:11 }, { f: [person:1, person:4, person:6], id: person:12 }]"

[[test.results]]
value = "[{ fof: [person:3], id: person:1 }, { fof: [person:4], id: person:2 }, { fof: [person:5], id: person:3 }, { fof: [person:6], id: person:4 }, { fof: [person:7], id: person:5 }, { fof: [person:8], id: person:6 }, { fof: [person:9], id: person:7 }, { fof: [person:10], id: person:8 }, { fof: [person:11], id: person:9 }, { fof: [person:12], id: person:10 }, { fof: [person:1], id: person:11 }, { fof: [person:2], id: person:12 }]"

[[test.results]]
value = "[{ fof: [person:6, person:9, person:11, person:8, person:11, person:1], id: person:1 }, { fof: [person:7, person:10, person:12, person:9, person:12, person:2], id: person:2 }, { fof: [person:8, person:11, person:1, person:10, person:1, person:3], id: person:3 }, { fof: [person:6, person:9, person:11, person:9, person:12, person:2, person:11, person:2, person:4], id: person:4 }, { fof: [person:7, person:10, person:12, person:10, person:1, person:3, person:12, person:3, person:5], id: person:5 }, { fof: [person:8, person:11, person:1, person:11, person:2, person:4, person:1, person:4, person:6], id: person:6 }, { fof: [person:9, person:12, person:2, person:12, person:3, person:5, person:5, person:7], id: person:7 }, { fof: [person:10, person:1, person:3, person:1, person:4, person:6, person:6, person:8], id: person:8 }, { fof: [person:11, person:2, person:4, person:5, person:7, person:7, person:9], id: person:9 }, { fof: [person:12, person:3, person:5, person:6, person:8, person:5, person:8, person:10], id: person:10 }, { fof: [person:1, person:4, person:6, person:7, person:9, person:6, person:9, person:11], id: person:11 }, { fof: [person:5, person:7, person:5, person:8, person:10, person:7, person:10, person:12], id: person:12 }]"


[[test.results]]
value = "[{ id: person:1, now: [person:5, person:7], then: [[person:5], [person:6], [person:7], [person:5, person:8], [person:6, person:9], [person:7, person:10], [person:8, person:11], [person:9, person:12], [person:10, person:1], [person:11, person:2], [person:12, person:3], [person:1, person:4]] }, { id: person:2, now: [person:6, person:8], then: [[person:5], [person:6], [person:7], [person:5, person:8], [person:6, person:9], [person:7, person:10], [person:8, person:11], [person:9, person:12], [person:10, person:1], [person:11, person:2], [person:12, person:3], [person:1, person:4]] }, { id: person:3, now: [person:7, person:9], then: [[person:5], [person:6], [person:7], [person:5, person:8], [person:6, person:9], [person:7, person:10], [person:8, person:11], [person:9, person:12], [person:10, person:1], [person:11, person:2], [person:12, person:3], [person:1, person:4]] }]"

[[test.results]]
value = "[[person:2], [person:3], [person:4], [person:5], [person:6], [person:7], [person:8], [person:9], [person:10], [person:11], [person:12], [person:1]]"
*/

DEFINE TABLE person SCHEMALESS;
DEFINE TABLE knows TYPE RELATION FROM person TO person;
FOR $i IN 1..=12 { CREATE type::record('person', $i) SET n = $i; };
FOR $i IN 1..=12 {
	LET $edge = type::record('knows', $i);
	RELATE (type::record('person', $i))->$edge->(type::record('person', $i % 12 + 1));
};

SLEEP 50ms;
LET $t1 = time::now();
SLEEP 50ms;

FOR $i IN 1..=12 {
	LET $edge = type::record('knows', 100 + $i);
	RELATE (type::record('person', $i))->$edge->(type::record('person', ($i + 3) % 12 + 1));
};
DELETE knows:1, knows:2, knows:3;

SLEEP 50ms;
LET $t2 = time::now();
SLEEP 50ms;

FOR $i IN 1..=12 {
	LET $edge = type::record('knows', 200 + $i);
	RELATE (type::record('person', $i))->$edge->(type::record('person', ($i + 5) % 12 + 1));
};

SELECT id, ->knows->person AS f FROM person VERSION $t1;
SELECT id, ->knows->person AS f FROM person VERSION $t2;
SELECT id, ->knows->person AS f FROM person;
SELECT id, ->knows->person->knows->person AS fof FROM person VERSION $t1;
SELECT id, ->knows->person->knows->person AS fof FROM person;
SELECT id, ->knows->person AS now, (SELECT VALUE ->knows->person FROM person VERSION $t2) AS then FROM person:1..=3;
SELECT VALUE (SELECT VALUE id FROM person)->knows->person FROM ONLY person:1 VERSION $t1;
