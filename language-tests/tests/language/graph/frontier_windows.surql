/**
[env]
namespace = true
database = true

[test]
reason = "Graph traversals over many sources read each hop's sources, and each batch of rows, together: results, edge order per origin and the order of the rows must be the ones the sources' reads one at a time give"

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
value = "[{ id: person:900 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: knows:99999, in: person:30, out: person:900 }]"

[[test.results]]
value = "[{ id: knows:99998, in: person:31, out: person:900 }]"

[[test.results]]
value = "[{ f: [person:4] }, { f: [person:7, person:8] }, { f: [person:10, person:11, person:12] }, { f: [person:13, person:14, person:15, person:16] }, { f: [] }, { f: [person:19] }, { f: [person:22, person:23] }, { f: [person:25, person:26, person:27] }, { f: [person:28, person:29, person:30, person:31] }, { f: [] }, { f: [person:34] }, { f: [person:37, person:38] }]"

[[test.results]]
value = "[{ fof: [person:13, person:14, person:15, person:16] }, { fof: [person:22, person:23, person:25, person:26, person:27] }, { fof: [person:34, person:37, person:38] }, { fof: [person:40, person:41, person:42, person:43, person:44, person:45, person:46, person:49] }, { fof: [] }, { fof: [person:58, person:59, person:60, person:1] }, { fof: [person:7, person:8, person:10, person:11, person:12] }, { fof: [person:19, person:22, person:23] }, { fof: [person:25, person:26, person:27, person:28, person:29, person:30, person:31, person:900, person:34, person:900] }, { fof: [] }, { fof: [person:43, person:44, person:45, person:46] }, { fof: [person:52, person:53, person:55, person:56, person:57] }]"

[[test.results]]
value = "[[], [], [person:13], [person:16], [], [], [], [person:28], [person:31], [], [], [], [person:43], [person:46], [], [], [], [person:58], [person:1], [], [], [], [person:13], [person:16], [], [], [], [person:28], [person:31], []]"

[[test.results]]
value = "[{ back: [person:19, person:39, person:59] }, { back: [] }, { back: [] }, { back: [person:1, person:21, person:41] }, { back: [] }, { back: [] }, { back: [person:2, person:22, person:42] }, { back: [person:2, person:22, person:42] }, { back: [] }, { back: [person:3, person:23, person:43] }, { back: [person:3, person:23, person:43] }, { back: [person:3, person:23, person:43] }, { back: [person:4, person:24, person:44] }, { back: [person:4, person:24, person:44] }, { back: [person:4, person:24, person:44] }]"

[[test.results]]
value = "[{ around: [person:19, person:1, person:39, person:1, person:59, person:1, person:1, person:4] }, { around: [person:2, person:7, person:2, person:8] }, { around: [person:3, person:10, person:3, person:11, person:3, person:12] }, { around: [person:1, person:4, person:21, person:4, person:41, person:4, person:4, person:13, person:4, person:14, person:4, person:15, person:4, person:16] }, { around: [] }, { around: [person:6, person:19] }, { around: [person:2, person:7, person:22, person:7, person:42, person:7, person:7, person:22, person:7, person:23] }, { around: [person:2, person:8, person:22, person:8, person:42, person:8, person:8, person:25, person:8, person:26, person:8, person:27] }, { around: [person:9, person:28, person:9, person:29, person:9, person:30, person:9, person:31] }, { around: [person:3, person:10, person:23, person:10, person:43, person:10] }]"

[[test.results]]
value = "[{ any: [person:4] }, { any: [person:7, person:8] }, { any: [person:10, person:11, person:12, person:4] }, { any: [person:13, person:14, person:15, person:16] }, { any: [] }, { any: [person:19, person:7] }, { any: [person:22, person:23] }, { any: [person:25, person:26, person:27] }, { any: [person:28, person:29, person:30, person:31, person:10] }, { any: [] }, { any: [person:34] }, { any: [person:37, person:38, person:13] }]"

[[test.results]]
value = "[700, 704, 5]"

[[test.results]]
value = "[0]"

[[test.results]]
value = "[700, 700, 700]"

[[test.results]]
value = "[700]"

[[test.results]]
value = "[{ f: [person:13, person:14, person:15, person:16] }, { f: [] }, { f: [person:19] }, { f: [person:22, person:23] }, { f: [person:25, person:26, person:27] }]"

[[test.results]]
value = "[{ f: [person:49] }, { f: [person:52, person:53] }, { f: [person:55, person:56, person:57] }, { f: [person:58, person:59, person:60, person:1] }, { f: [] }]"

[[test.results]]
value = "[{ f: [] }, { f: [person:7, person:8] }, { f: [] }, { f: [person:13, person:14, person:15, person:16] }, { f: [] }, { f: [person:19] }, { f: [] }, { f: [person:25, person:26, person:27] }, { f: [] }, { f: [] }]"

[[test.results]]
value = "[{ n: 26, ns: [19] }, { n: 25, ns: [] }, { n: 24, ns: [13, 14, 15, 16] }, { n: 23, ns: [10, 11, 12] }, { n: 22, ns: [7, 8] }, { n: 21, ns: [4] }, { n: 20, ns: [] }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[person:10, person:11, person:12]]"
*/

-- 0: 60 people, up to four `knows` edges each (keys of their own) and some `likes`; and a hub,
-- person:900, that two of them know and that holds 700 edges.
DEFINE TABLE person SCHEMALESS;
DEFINE TABLE knows TYPE RELATION;
DEFINE TABLE likes TYPE RELATION;
FOR $i IN 1..=60 { CREATE type::record('person', $i) SET n = $i; };
FOR $i IN 1..=60 {
	FOR $k IN 0..($i % 5) {
		LET $edge = type::record('knows', $i * 10 + $k);
		RELATE (type::record('person', $i))->$edge->(type::record('person', ($i * 3 + $k) % 60 + 1));
	};
	IF $i % 3 = 0 {
		LET $like = type::record('likes', $i);
		RELATE (type::record('person', $i))->$like->(type::record('person', $i % 60 + 1));
	};
};
CREATE person:900;
FOR $i IN 1..=700 {
	LET $edge = type::record('knows', 100000 + $i);
	CREATE type::record('person', 1000 + $i);
	RELATE person:900->$edge->(type::record('person', 1000 + $i));
};
RELATE person:30->knows:99999->person:900;
RELATE person:31->knows:99998->person:900;

-- One hop from each person, and two hops, whose second hop reads the edges of the first as sources
SELECT ->knows->person AS f FROM person LIMIT 12;
SELECT ->knows->person->knows->person AS fof FROM person:1..=12;
SELECT VALUE ->knows->person->likes->person FROM person:1..=30;
SELECT <-knows<-person AS back FROM person:1..=15;
SELECT <->knows<->person AS around FROM person:1..=10;
SELECT ->(knows, likes)->person AS any FROM person:1..=12;

-- Hops through a source that holds more edges than one window read holds, as a source of a hop
-- and as one of the rows of a batch
SELECT VALUE array::len(->knows->person->knows->person) FROM person:30..=32;
SELECT VALUE array::len(->knows->person->knows->person->knows->person) FROM person:30;
SELECT VALUE array::len(<-knows<-person->knows->person) FROM person:1001..=1003;
SELECT VALUE array::len(->knows->person) FROM person:895..=905;

-- Paging, a filter between the rows and the lookups, and a lookup that runs on some rows only
SELECT ->knows->person AS f FROM person LIMIT 5 START 3;
SELECT ->knows->person AS f FROM person WHERE n > 55;
SELECT IF n % 2 = 0 { ->knows->person } ELSE { [] } AS f FROM person:1..=10;
SELECT n, ->knows->person.n AS ns FROM person:20..=26 ORDER BY n DESC;

-- A traversal that follows a write of its own transaction
BEGIN;
RELATE person:3->knows:3000->person:4;
DELETE knows:30;
RETURN (SELECT VALUE ->knows->person->knows->person FROM person:3);
CANCEL;
SELECT VALUE ->knows->person FROM person:3;
