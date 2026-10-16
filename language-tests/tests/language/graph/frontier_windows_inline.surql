/**
[env]
namespace = true
database = true

[test]
reason = "Graph and reference traversals over many sources and rows read the inline adjacency and reference caches of the sources, and the keys of the ranges no live cache answers, together: results and the order of the edges and references per origin must be the ones the sources' reads one at a time give, for caches that hold their whole set, for a hub whose set outgrew the cap of a cache, and for the paths that cross from one kind of read to the other"

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
value = "NONE"

[[test.results]]
value = "[{ f: [person:4, person:11, person:12, person:13, person:14, person:15, person:16, person:17, person:18, person:19, person:20, person:21, person:22], id: person:1 }, { f: [person:7, person:8], id: person:2 }, { f: [person:10, person:11, person:12], id: person:3 }, { f: [], id: person:4 }, { f: [person:16], id: person:5 }, { f: [person:19, person:20], id: person:6 }, { f: [person:22, person:23, person:24], id: person:7 }, { f: [], id: person:8 }, { f: [person:28], id: person:9 }, { f: [person:1, person:2], id: person:10 }, { f: [person:4, person:5, person:6], id: person:11 }, { f: [], id: person:12 }, { f: [person:10], id: person:13 }, { f: [person:13, person:14], id: person:14 }, { f: [person:16, person:17, person:18], id: person:15 }, { f: [], id: person:16 }, { f: [person:22], id: person:17 }, { f: [person:25, person:26], id: person:18 }, { f: [person:28, person:29, person:30], id: person:19 }, { f: [], id: person:20 }, { f: [person:4], id: person:21 }, { f: [person:7, person:8], id: person:22 }, { f: [person:10, person:11, person:12], id: person:23 }, { f: [], id: person:24 }, { f: [person:16], id: person:25 }, { f: [person:19, person:20], id: person:26 }, { f: [person:22, person:23, person:24], id: person:27 }, { f: [], id: person:28 }, { f: [person:28], id: person:29 }, { f: [person:1, person:2], id: person:30 }]"

[[test.results]]
value = "[{ b: [person:10, person:30], id: person:1 }, { b: [person:10, person:30], id: person:2 }, { b: [], id: person:3 }, { b: [person:1, person:11, person:21], id: person:4 }, { b: [person:11], id: person:5 }, { b: [person:11], id: person:6 }, { b: [person:2, person:22], id: person:7 }, { b: [person:2, person:22], id: person:8 }, { b: [], id: person:9 }, { b: [person:3, person:13, person:23], id: person:10 }, { b: [person:3, person:23, person:1], id: person:11 }, { b: [person:3, person:23, person:1], id: person:12 }, { b: [person:14, person:1], id: person:13 }, { b: [person:14, person:1], id: person:14 }, { b: [person:1], id: person:15 }, { b: [person:5, person:15, person:25, person:1], id: person:16 }, { b: [person:15, person:1], id: person:17 }, { b: [person:15, person:1], id: person:18 }, { b: [person:6, person:26, person:1], id: person:19 }, { b: [person:6, person:26, person:1], id: person:20 }, { b: [person:1], id: person:21 }, { b: [person:7, person:17, person:27, person:1], id: person:22 }, { b: [person:7, person:27], id: person:23 }, { b: [person:7, person:27], id: person:24 }, { b: [person:18], id: person:25 }, { b: [person:18], id: person:26 }, { b: [], id: person:27 }, { b: [person:9, person:19, person:29], id: person:28 }, { b: [person:19], id: person:29 }, { b: [person:19], id: person:30 }]"

[[test.results]]
value = "[{ any: [person:10, person:1, person:30, person:1, person:1, person:4, person:1, person:11, person:1, person:12, person:1, person:13, person:1, person:14, person:1, person:15, person:1, person:16, person:1, person:17, person:1, person:18, person:1, person:19, person:1, person:20, person:1, person:21, person:1, person:22], id: person:1 }, { any: [person:10, person:2, person:30, person:2, person:2, person:7, person:2, person:8], id: person:2 }, { any: [person:3, person:10, person:3, person:11, person:3, person:12], id: person:3 }, { any: [person:1, person:4, person:11, person:4, person:21, person:4], id: person:4 }, { any: [person:11, person:5, person:5, person:16], id: person:5 }, { any: [person:11, person:6, person:6, person:19, person:6, person:20], id: person:6 }, { any: [person:2, person:7, person:22, person:7, person:7, person:22, person:7, person:23, person:7, person:24], id: person:7 }, { any: [person:2, person:8, person:22, person:8], id: person:8 }, { any: [person:9, person:28], id: person:9 }, { any: [person:3, person:10, person:13, person:10, person:23, person:10, person:10, person:1, person:10, person:2], id: person:10 }, { any: [person:3, person:11, person:23, person:11, person:1, person:11, person:11, person:4, person:11, person:5, person:11, person:6], id: person:11 }, { any: [person:3, person:12, person:23, person:12, person:1, person:12], id: person:12 }, { any: [person:14, person:13, person:1, person:13, person:13, person:10], id: person:13 }, { any: [person:14, person:14, person:1, person:14, person:14, person:13, person:14, person:14], id: person:14 }, { any: [person:1, person:15, person:15, person:16, person:15, person:17, person:15, person:18], id: person:15 }, { any: [person:5, person:16, person:15, person:16, person:25, person:16, person:1, person:16], id: person:16 }, { any: [person:15, person:17, person:1, person:17, person:17, person:22], id: person:17 }, { any: [person:15, person:18, person:1, person:18, person:18, person:25, person:18, person:26], id: person:18 }, { any: [person:6, person:19, person:26, person:19, person:1, person:19, person:19, person:28, person:19, person:29, person:19, person:30], id: person:19 }, { any: [person:6, person:20, person:26, person:20, person:1, person:20], id: person:20 }, { any: [person:1, person:21, person:21, person:4], id: person:21 }, { any: [person:7, person:22, person:17, person:22, person:27, person:22, person:1, person:22, person:22, person:7, person:22, person:8], id: person:22 }, { any: [person:7, person:23, person:27, person:23, person:23, person:10, person:23, person:11, person:23, person:12], id: person:23 }, { any: [person:7, person:24, person:27, person:24], id: person:24 }, { any: [person:18, person:25, person:25, person:16], id: person:25 }, { any: [person:18, person:26, person:26, person:19, person:26, person:20], id: person:26 }, { any: [person:27, person:22, person:27, person:23, person:27, person:24], id: person:27 }, { any: [person:9, person:28, person:19, person:28, person:29, person:28], id: person:28 }, { any: [person:19, person:29, person:29, person:28], id: person:29 }, { any: [person:19, person:30, person:30, person:1, person:30, person:2], id: person:30 }]"

[[test.results]]
value = "[{ fof: [person:4, person:5, person:6, person:10, person:13, person:14, person:16, person:17, person:18, person:22, person:25, person:26, person:28, person:29, person:30, person:4, person:7, person:8], id: person:1 }, { fof: [person:22, person:23, person:24], id: person:2 }, { fof: [person:1, person:2, person:4, person:5, person:6], id: person:3 }, { fof: [], id: person:4 }, { fof: [], id: person:5 }, { fof: [person:28, person:29, person:30], id: person:6 }, { fof: [person:7, person:8, person:10, person:11, person:12], id: person:7 }, { fof: [], id: person:8 }, { fof: [], id: person:9 }, { fof: [person:4, person:11, person:12, person:13, person:14, person:15, person:16, person:17, person:18, person:19, person:20, person:21, person:22, person:7, person:8], id: person:10 }, { fof: [person:16, person:19, person:20], id: person:11 }, { fof: [], id: person:12 }, { fof: [person:1, person:2], id: person:13 }, { fof: [person:10, person:13, person:14], id: person:14 }, { fof: [person:22, person:25, person:26], id: person:15 }, { fof: [], id: person:16 }, { fof: [person:7, person:8], id: person:17 }, { fof: [person:16, person:19, person:20], id: person:18 }, { fof: [person:28, person:1, person:2], id: person:19 }, { fof: [], id: person:20 }, { fof: [], id: person:21 }, { fof: [person:22, person:23, person:24], id: person:22 }, { fof: [person:1, person:2, person:4, person:5, person:6], id: person:23 }, { fof: [], id: person:24 }, { fof: [], id: person:25 }, { fof: [person:28, person:29, person:30], id: person:26 }, { fof: [person:7, person:8, person:10, person:11, person:12], id: person:27 }, { fof: [], id: person:28 }, { fof: [], id: person:29 }, { fof: [person:4, person:11, person:12, person:13, person:14, person:15, person:16, person:17, person:18, person:19, person:20, person:21, person:22, person:7, person:8], id: person:30 }]"

[[test.results]]
value = "[{ id: person:1, notes: [note:10] }, { id: person:2, notes: [note:20, note:21, note:801, note:802, note:803, note:804, note:805, note:806, note:807, note:808, note:809, note:810, note:811, note:812] }, { id: person:3, notes: [] }, { id: person:4, notes: [note:40] }, { id: person:5, notes: [note:50, note:51] }, { id: person:6, notes: [] }, { id: person:7, notes: [note:70] }, { id: person:8, notes: [note:80, note:81] }, { id: person:9, notes: [] }, { id: person:10, notes: [note:100] }, { id: person:11, notes: [note:110, note:111] }, { id: person:12, notes: [] }, { id: person:13, notes: [note:130] }, { id: person:14, notes: [note:140, note:141] }, { id: person:15, notes: [] }, { id: person:16, notes: [note:160] }, { id: person:17, notes: [note:170, note:171] }, { id: person:18, notes: [] }, { id: person:19, notes: [note:190] }, { id: person:20, notes: [note:200, note:201] }, { id: person:21, notes: [] }, { id: person:22, notes: [note:220] }, { id: person:23, notes: [note:230, note:231] }, { id: person:24, notes: [] }, { id: person:25, notes: [note:250] }, { id: person:26, notes: [note:260, note:261] }, { id: person:27, notes: [] }, { id: person:28, notes: [note:280] }, { id: person:29, notes: [note:290, note:291] }, { id: person:30, notes: [] }]"

[[test.results]]
value = "[{ id: person:1, reached_notes: [note:40, note:110, note:111, note:130, note:140, note:141, note:160, note:170, note:171, note:190, note:200, note:201, note:220] }, { id: person:2, reached_notes: [note:70, note:80, note:81] }, { id: person:3, reached_notes: [note:100, note:110, note:111] }, { id: person:4, reached_notes: [] }, { id: person:5, reached_notes: [note:160] }, { id: person:6, reached_notes: [note:190, note:200, note:201] }, { id: person:7, reached_notes: [note:220, note:230, note:231] }, { id: person:8, reached_notes: [] }, { id: person:9, reached_notes: [note:280] }, { id: person:10, reached_notes: [note:10, note:20, note:21, note:801, note:802, note:803, note:804, note:805, note:806, note:807, note:808, note:809, note:810, note:811, note:812] }, { id: person:11, reached_notes: [note:40, note:50, note:51] }, { id: person:12, reached_notes: [] }, { id: person:13, reached_notes: [note:100] }, { id: person:14, reached_notes: [note:130, note:140, note:141] }, { id: person:15, reached_notes: [note:160, note:170, note:171] }, { id: person:16, reached_notes: [] }, { id: person:17, reached_notes: [note:220] }, { id: person:18, reached_notes: [note:250, note:260, note:261] }, { id: person:19, reached_notes: [note:280, note:290, note:291] }, { id: person:20, reached_notes: [] }, { id: person:21, reached_notes: [note:40] }, { id: person:22, reached_notes: [note:70, note:80, note:81] }, { id: person:23, reached_notes: [note:100, note:110, note:111] }, { id: person:24, reached_notes: [] }, { id: person:25, reached_notes: [note:160] }, { id: person:26, reached_notes: [note:190, note:200, note:201] }, { id: person:27, reached_notes: [note:220, note:230, note:231] }, { id: person:28, reached_notes: [] }, { id: person:29, reached_notes: [note:280] }, { id: person:30, reached_notes: [note:10, note:20, note:21, note:801, note:802, note:803, note:804, note:805, note:806, note:807, note:808, note:809, note:810, note:811, note:812] }]"

[[test.results]]
value = "[{ from_notes: [], id: person:1 }, { from_notes: [], id: person:2 }, { from_notes: [], id: person:3 }, { from_notes: [], id: person:4 }, { from_notes: [], id: person:5 }, { from_notes: [], id: person:6 }, { from_notes: [], id: person:7 }, { from_notes: [], id: person:8 }, { from_notes: [], id: person:9 }, { from_notes: [], id: person:10 }, { from_notes: [], id: person:11 }, { from_notes: [], id: person:12 }, { from_notes: [], id: person:13 }, { from_notes: [], id: person:14 }, { from_notes: [], id: person:15 }, { from_notes: [], id: person:16 }, { from_notes: [], id: person:17 }, { from_notes: [], id: person:18 }, { from_notes: [], id: person:19 }, { from_notes: [], id: person:20 }, { from_notes: [], id: person:21 }, { from_notes: [], id: person:22 }, { from_notes: [], id: person:23 }, { from_notes: [], id: person:24 }, { from_notes: [], id: person:25 }, { from_notes: [], id: person:26 }, { from_notes: [], id: person:27 }, { from_notes: [], id: person:28 }, { from_notes: [], id: person:29 }, { from_notes: [], id: person:30 }]"

[[test.results]]
value = "[{ edges: 13, id: person:1, notes: 1 }, { edges: 2, id: person:2, notes: 14 }, { edges: 3, id: person:3, notes: 0 }, { edges: 0, id: person:4, notes: 1 }, { edges: 1, id: person:5, notes: 2 }, { edges: 2, id: person:6, notes: 0 }, { edges: 3, id: person:7, notes: 1 }, { edges: 0, id: person:8, notes: 2 }, { edges: 1, id: person:9, notes: 0 }, { edges: 2, id: person:10, notes: 1 }, { edges: 3, id: person:11, notes: 2 }, { edges: 0, id: person:12, notes: 0 }, { edges: 1, id: person:13, notes: 1 }, { edges: 2, id: person:14, notes: 2 }, { edges: 3, id: person:15, notes: 0 }, { edges: 0, id: person:16, notes: 1 }, { edges: 1, id: person:17, notes: 2 }, { edges: 2, id: person:18, notes: 0 }, { edges: 3, id: person:19, notes: 1 }, { edges: 0, id: person:20, notes: 2 }, { edges: 1, id: person:21, notes: 0 }, { edges: 2, id: person:22, notes: 1 }, { edges: 3, id: person:23, notes: 2 }, { edges: 0, id: person:24, notes: 0 }, { edges: 1, id: person:25, notes: 1 }, { edges: 2, id: person:26, notes: 2 }, { edges: 3, id: person:27, notes: 0 }, { edges: 0, id: person:28, notes: 1 }, { edges: 1, id: person:29, notes: 2 }, { edges: 2, id: person:30, notes: 0 }]"

[[test.results]]
value = "[[person:4, person:11, person:12, person:13, person:14, person:15, person:16, person:17, person:18, person:19, person:20, person:21, person:22], [person:7, person:8], [person:10, person:11, person:12], [], [person:16], [person:19, person:20], [person:22, person:23, person:24], [], [person:28], [person:1, person:2], [person:4, person:5, person:6], [], [person:10], [person:13, person:14], [person:16, person:17, person:18], [], [person:22], [person:25, person:26], [person:28, person:29, person:30], [], [person:4], [person:7, person:8], [person:10, person:11, person:12], [], [person:16], [person:19, person:20], [person:22, person:23, person:24], [], [person:28], [person:1, person:2]]"

[[test.results]]
value = "[[[person:4, person:11, person:12, person:13, person:14, person:15, person:16, person:17, person:18, person:19, person:20, person:21, person:22], [person:7, person:8], [person:10, person:11, person:12], [], [person:16], [person:19, person:20], [person:22, person:23, person:24], [], [person:28], [person:1, person:2], [person:4, person:5, person:6], [], [person:10], [person:13, person:14], [person:16, person:17, person:18], [], [person:22], [person:25, person:26], [person:28, person:29, person:30], [], [person:4], [person:7, person:8], [person:10, person:11, person:12], [], [person:16], [person:19, person:20], [person:22, person:23, person:24], [], [person:28], [person:1, person:2]]]"

[[test.results]]
value = "[[[person:4, person:5, person:6, person:10, person:13, person:14, person:16, person:17, person:18, person:22, person:25, person:26, person:28, person:29, person:30, person:4, person:7, person:8], [person:22, person:23, person:24], [person:1, person:2, person:4, person:5, person:6], [], [], [person:28, person:29, person:30], [person:7, person:8, person:10, person:11, person:12], [], [], [person:4, person:11, person:12, person:13, person:14, person:15, person:16, person:17, person:18, person:19, person:20, person:21, person:22, person:7, person:8], [person:16, person:19, person:20], [], [person:1, person:2], [person:10, person:13, person:14], [person:22, person:25, person:26], [], [person:7, person:8], [person:16, person:19, person:20], [person:28, person:1, person:2], [], [], [person:22, person:23, person:24], [person:1, person:2, person:4, person:5, person:6], [], [], [person:28, person:29, person:30], [person:7, person:8, person:10, person:11, person:12], [], [], [person:4, person:11, person:12, person:13, person:14, person:15, person:16, person:17, person:18, person:19, person:20, person:21, person:22, person:7, person:8]]]"

[[test.results]]
value = "[[note:10, note:20, note:21, note:801, note:802, note:803, note:804, note:805, note:806, note:807, note:808, note:809, note:810, note:811, note:812, note:40, note:50, note:51, note:70, note:80, note:81, note:100, note:110, note:111, note:130, note:140, note:141, note:160, note:170, note:171, note:190, note:200, note:201, note:220, note:230, note:231, note:250, note:260, note:261, note:280, note:290, note:291]]"

[[test.results]]
value = "[{ f: [], id: person:28 }, { f: [person:22, person:23, person:24], id: person:27 }, { f: [person:19, person:20], id: person:26 }, { f: [person:16], id: person:25 }, { f: [], id: person:24 }, { f: [person:10, person:11, person:12], id: person:23 }, { f: [person:7, person:8], id: person:22 }]"
*/

DEFINE TABLE person SCHEMALESS INLINE EDGES 8 INLINE REFERENCES 8;
DEFINE TABLE knows TYPE RELATION FROM person TO person;
DEFINE TABLE note SCHEMALESS;
DEFINE FIELD author ON note TYPE record<person> REFERENCE;
FOR $i IN 1..=30 { CREATE type::record('person', $i) SET n = $i; };
FOR $i IN 1..=30 {
	FOR $k IN 0..($i % 4) {
		LET $edge = type::record('knows', $i * 10 + $k);
		RELATE (type::record('person', $i))->$edge->(type::record('person', ($i * 3 + $k) % 30 + 1));
	};
};
-- A hub whose adjacency outgrows the cap of eight inline entries.
FOR $j IN 1..=12 {
	LET $edge = type::record('knows', 900 + $j);
	RELATE person:1->$edge->(type::record('person', $j + 10));
};
FOR $i IN 1..=30 {
	FOR $k IN 0..($i % 3) {
		CREATE type::record('note', $i * 10 + $k) SET author = type::record('person', $i);
	};
};
-- A hub of references that outgrows the cap too.
FOR $j IN 1..=12 { CREATE type::record('note', 800 + $j) SET author = person:2; };

SELECT id, ->knows->person AS f FROM person;
SELECT id, <-knows<-person AS b FROM person;
SELECT id, <->knows<->person AS any FROM person;
SELECT id, ->knows->person->knows->person AS fof FROM person;
SELECT id, <~note AS notes FROM person;
SELECT id, ->knows->person<~note AS reached_notes FROM person;
SELECT id, <~note->knows->person AS from_notes FROM person;
SELECT id, array::len(->knows) AS edges, array::len(<~note) AS notes FROM person;
SELECT VALUE ->knows->person FROM person:1..=30;
SELECT VALUE (SELECT VALUE id FROM person)->knows->person FROM person:1;
SELECT VALUE (SELECT VALUE id FROM person)->knows->person->knows->person FROM person:1;
SELECT VALUE (SELECT VALUE id FROM person)<~note FROM person:1;
SELECT id, ->knows->person AS f FROM person ORDER BY n DESC LIMIT 7 START 2;
