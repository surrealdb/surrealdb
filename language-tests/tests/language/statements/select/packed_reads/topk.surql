/**
[env]
namespace = true
database = true
timeout = 30000

[test]
reason = "A top-k scan of a SCHEMAFULL table that is read in batches returns the same rows in the same order whichever kind of column it orders by, with ties, NONE and NULL keys, several keys and a filter."

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
value = "[{ i: 0, id: item:0 }, { i: 0, id: item:40 }, { i: 0, id: item:80 }, { i: 0, id: item:120 }, { i: 0, id: item:160 }]"

[[test.results]]
value = "[{ i: 39, id: item:39 }, { i: 39, id: item:79 }, { i: 39, id: item:119 }, { i: 39, id: item:159 }, { i: 39, id: item:199 }]"

[[test.results]]
value = "[{ id: item:0, s: 's0' }, { id: item:17, s: 's0' }, { id: item:34, s: 's0' }, { id: item:51, s: 's0' }, { id: item:68, s: 's0' }]"

[[test.results]]
value = "[{ id: item:9, s: 's9' }, { id: item:26, s: 's9' }, { id: item:43, s: 's9' }, { id: item:60, s: 's9' }, { id: item:77, s: 's9' }]"

[[test.results]]
value = "[{ f: 0f, id: item:0 }, { f: 0f, id: item:23 }, { f: 0f, id: item:46 }, { f: 0f, id: item:69 }, { f: 0f, id: item:92 }]"

[[test.results]]
value = "[{ f: 5.5f, id: item:22 }, { f: 5.5f, id: item:45 }, { f: 5.5f, id: item:68 }, { f: 5.5f, id: item:91 }, { f: 5.5f, id: item:114 }]"

[[test.results]]
value = "[{ b: false, id: item:1 }, { b: false, id: item:2 }, { b: false, id: item:4 }, { b: false, id: item:5 }, { b: false, id: item:7 }]"

[[test.results]]
value = "[{ b: true, id: item:0 }, { b: true, id: item:3 }, { b: true, id: item:6 }, { b: true, id: item:9 }, { b: true, id: item:12 }]"

[[test.results]]
value = "[{ dec: 0dec, id: item:0 }, { dec: 0dec, id: item:31 }, { dec: 0dec, id: item:62 }, { dec: 0dec, id: item:93 }, { dec: 0dec, id: item:124 }]"

[[test.results]]
value = "[{ dec: 30dec, id: item:30 }, { dec: 30dec, id: item:61 }, { dec: 30dec, id: item:92 }, { dec: 30dec, id: item:123 }, { dec: 30dec, id: item:154 }]"

[[test.results]]
value = "[{ id: item:0, t: d'2020-01-01T00:00:00Z' }, { id: item:50, t: d'2020-01-01T00:00:00Z' }, { id: item:100, t: d'2020-01-01T00:00:00Z' }, { id: item:150, t: d'2020-01-01T00:00:00Z' }, { id: item:200, t: d'2020-01-01T00:00:00Z' }]"

[[test.results]]
value = "[{ id: item:49, t: d'2020-02-19T00:00:00Z' }, { id: item:99, t: d'2020-02-19T00:00:00Z' }, { id: item:149, t: d'2020-02-19T00:00:00Z' }, { id: item:199, t: d'2020-02-19T00:00:00Z' }, { id: item:249, t: d'2020-02-19T00:00:00Z' }]"

[[test.results]]
value = "[{ du: 0ns, id: item:0 }, { du: 0ns, id: item:9 }, { du: 0ns, id: item:18 }, { du: 0ns, id: item:27 }, { du: 0ns, id: item:36 }]"

[[test.results]]
value = "[{ du: 8h, id: item:8 }, { du: 8h, id: item:17 }, { du: 8h, id: item:26 }, { du: 8h, id: item:35 }, { du: 8h, id: item:44 }]"

[[test.results]]
value = "[{ id: item:0, o: NONE }, { id: item:4, o: NONE }, { id: item:8, o: NONE }, { id: item:12, o: NONE }, { id: item:16, o: NONE }]"

[[test.results]]
value = "[{ id: item:25, o: 12 }, { id: item:38, o: 12 }, { id: item:51, o: 12 }, { id: item:77, o: 12 }, { id: item:90, o: 12 }]"

[[test.results]]
value = "[{ id: item:0, nl: NULL }, { id: item:5, nl: NULL }, { id: item:10, nl: NULL }, { id: item:15, nl: NULL }, { id: item:20, nl: NULL }]"

[[test.results]]
value = "[{ id: item:21, nl: 10 }, { id: item:32, nl: 10 }, { id: item:43, nl: 10 }, { id: item:54, nl: 10 }, { id: item:76, nl: 10 }]"

[[test.results]]
value = "[{ id: item:0, obj: { x: 0 } }, { id: item:9, obj: { x: 0 } }, { id: item:18, obj: { x: 0 } }, { id: item:27, obj: { x: 0 } }, { id: item:36, obj: { x: 0 } }]"

[[test.results]]
value = "[{ id: item:8, obj: { x: 8 } }, { id: item:17, obj: { x: 8 } }, { id: item:26, obj: { x: 8 } }, { id: item:35, obj: { x: 8 } }, { id: item:44, obj: { x: 8 } }]"

[[test.results]]
value = "[{ id: item:0, obj: { y: NONE } }, { id: item:6, obj: { y: NONE } }, { id: item:12, obj: { y: NONE } }, { id: item:18, obj: { y: NONE } }, { id: item:24, obj: { y: NONE } }]"

[[test.results]]
value = "[{ id: item:2, obj: { y: 'y2' } }, { id: item:5, obj: { y: 'y2' } }, { id: item:8, obj: { y: 'y2' } }, { id: item:11, obj: { y: 'y2' } }, { id: item:14, obj: { y: 'y2' } }]"

[[test.results]]
value = "[{ flex: { k: NONE }, id: item:1 }, { flex: { k: NONE }, id: item:2 }, { flex: { k: NONE }, id: item:3 }, { flex: { k: NONE }, id: item:4 }, { flex: { k: NONE }, id: item:5 }]"

[[test.results]]
value = "[{ flex: { k: 4 }, id: item:14 }, { flex: { k: 4 }, id: item:49 }, { flex: { k: 4 }, id: item:84 }, { flex: { k: 4 }, id: item:119 }, { flex: { k: 4 }, id: item:154 }]"

[[test.results]]
value = "[{ id: item:0, tags: ['a', 'b'] }, { id: item:2, tags: ['a', 'b'] }, { id: item:4, tags: ['a', 'b'] }, { id: item:6, tags: ['a', 'b'] }, { id: item:8, tags: ['a', 'b'] }]"

[[test.results]]
value = "[{ id: item:1, tags: ['c'] }, { id: item:3, tags: ['c'] }, { id: item:5, tags: ['c'] }, { id: item:7, tags: ['c'] }, { id: item:9, tags: ['c'] }]"

[[test.results]]
value = "[{ i: 0, id: item:0 }, { i: 0, id: item:40 }, { i: 0, id: item:80 }, { i: 0, id: item:120 }, { i: 0, id: item:160 }, { i: 0, id: item:200 }, { i: 0, id: item:240 }, { i: 0, id: item:280 }, { i: 0, id: item:320 }, { i: 0, id: item:360 }, { i: 0, id: item:400 }, { i: 0, id: item:440 }, { i: 0, id: item:480 }, { i: 0, id: item:520 }, { i: 0, id: item:560 }, { i: 0, id: item:600 }, { i: 0, id: item:640 }, { i: 0, id: item:680 }, { i: 0, id: item:720 }, { i: 0, id: item:760 }, { i: 0, id: item:800 }, { i: 0, id: item:840 }, { i: 0, id: item:880 }, { i: 0, id: item:920 }, { i: 0, id: item:960 }, { i: 0, id: item:1000 }, { i: 0, id: item:1040 }, { i: 0, id: item:1080 }, { i: 0, id: item:1120 }, { i: 0, id: item:1160 }, { i: 1, id: item:1 }, { i: 1, id: item:41 }, { i: 1, id: item:81 }, { i: 1, id: item:121 }, { i: 1, id: item:161 }, { i: 1, id: item:201 }, { i: 1, id: item:241 }, { i: 1, id: item:281 }, { i: 1, id: item:321 }, { i: 1, id: item:361 }, { i: 1, id: item:401 }, { i: 1, id: item:441 }, { i: 1, id: item:481 }, { i: 1, id: item:521 }, { i: 1, id: item:561 }, { i: 1, id: item:601 }, { i: 1, id: item:641 }, { i: 1, id: item:681 }, { i: 1, id: item:721 }, { i: 1, id: item:761 }]"

[[test.results]]
value = "[{ i: 39, id: item:159 }, { i: 39, id: item:199 }, { i: 39, id: item:239 }, { i: 39, id: item:279 }, { i: 39, id: item:319 }, { i: 39, id: item:359 }, { i: 39, id: item:399 }, { i: 39, id: item:439 }, { i: 39, id: item:479 }, { i: 39, id: item:519 }, { i: 39, id: item:559 }, { i: 39, id: item:599 }, { i: 39, id: item:639 }, { i: 39, id: item:679 }, { i: 39, id: item:719 }, { i: 39, id: item:759 }, { i: 39, id: item:799 }, { i: 39, id: item:839 }, { i: 39, id: item:879 }, { i: 39, id: item:919 }, { i: 39, id: item:959 }, { i: 39, id: item:999 }, { i: 39, id: item:1039 }, { i: 39, id: item:1079 }, { i: 39, id: item:1119 }, { i: 39, id: item:1159 }, { i: 39, id: item:1199 }, { i: 38, id: item:38 }, { i: 38, id: item:78 }, { i: 38, id: item:118 }, { i: 38, id: item:158 }, { i: 38, id: item:198 }, { i: 38, id: item:238 }, { i: 38, id: item:278 }, { i: 38, id: item:318 }, { i: 38, id: item:358 }, { i: 38, id: item:398 }, { i: 38, id: item:438 }, { i: 38, id: item:478 }, { i: 38, id: item:518 }, { i: 38, id: item:558 }, { i: 38, id: item:598 }, { i: 38, id: item:638 }, { i: 38, id: item:678 }, { i: 38, id: item:718 }, { i: 38, id: item:758 }, { i: 38, id: item:798 }, { i: 38, id: item:838 }, { i: 38, id: item:878 }, { i: 38, id: item:918 }]"

[[test.results]]
value = "[{ i: 0, id: item:0, s: 's0' }, { i: 0, id: item:680, s: 's0' }, { i: 0, id: item:120, s: 's1' }, { i: 0, id: item:800, s: 's1' }, { i: 0, id: item:520, s: 's10' }]"

[[test.results]]
value = "[{ i: 0, id: item:400, s: 's9' }, { i: 0, id: item:1080, s: 's9' }, { i: 1, id: item:281, s: 's9' }, { i: 1, id: item:961, s: 's9' }, { i: 2, id: item:162, s: 's9' }]"

[[test.results]]
value = "[{ i: 1, id: item:241, nl: 10 }, { i: 1, id: item:681, nl: 10 }, { i: 1, id: item:1121, nl: 10 }, { i: 2, id: item:362, nl: 10 }, { i: 2, id: item:802, nl: 10 }]"

[[test.results]]
value = "[{ id: item:32, nl: 10, o: NONE }, { id: item:76, nl: 10, o: NONE }, { id: item:164, nl: 10, o: NONE }, { id: item:208, nl: 10, o: NONE }, { id: item:252, nl: 10, o: NONE }, { id: item:296, nl: 10, o: NONE }, { id: item:384, nl: 10, o: NONE }]"

[[test.results]]
value = "[{ b: true, f: 0f, id: item:138 }, { b: true, f: 0f, id: item:207 }, { b: true, f: 0f, id: item:276 }, { b: true, f: 0f, id: item:345 }, { b: true, f: 0f, id: item:414 }, { b: true, f: 0f, id: item:483 }, { b: true, f: 0f, id: item:552 }]"

[[test.results]]
value = "[{ id: item:36, o: NONE }, { id: item:76, o: NONE }, { id: item:116, o: NONE }, { id: item:156, o: NONE }, { id: item:196, o: NONE }]"

[[test.results]]
value = "[{ dec: 0dec, id: item:93 }, { dec: 0dec, id: item:186 }, { dec: 0dec, id: item:279 }, { dec: 0dec, id: item:372 }, { dec: 0dec, id: item:465 }]"

[[test.results]]
value = "[{ id: item:45, t: d'2020-02-15T00:00:00Z' }, { id: item:95, t: d'2020-02-15T00:00:00Z' }, { id: item:145, t: d'2020-02-15T00:00:00Z' }, { id: item:195, t: d'2020-02-15T00:00:00Z' }, { id: item:245, t: d'2020-02-15T00:00:00Z' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ i: 39, id: item:1039 }, { i: 39, id: item:1079 }, { i: 39, id: item:1119 }, { i: 39, id: item:1159 }, { i: 39, id: item:1199 }]"

*/

DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD i ON item TYPE int;
DEFINE FIELD s ON item TYPE string;
DEFINE FIELD f ON item TYPE float;
DEFINE FIELD b ON item TYPE bool;
DEFINE FIELD dec ON item TYPE decimal;
DEFINE FIELD t ON item TYPE datetime;
DEFINE FIELD du ON item TYPE duration;
DEFINE FIELD o ON item TYPE option<int>;
DEFINE FIELD nl ON item TYPE int | null;
DEFINE FIELD tags ON item TYPE array<string>;
DEFINE FIELD obj ON item TYPE object;
DEFINE FIELD obj.x ON item TYPE int;
DEFINE FIELD obj.y ON item TYPE option<string>;
DEFINE FIELD flex ON item TYPE object FLEXIBLE;
FOR $n IN 0..1200 {
	CREATE type::record('item', $n) CONTENT {
		i: $n % 40,
		s: 's' + <string> ($n % 17),
		f: <float> ($n % 23) / 4,
		b: $n % 3 = 0,
		dec: <decimal> ($n % 31),
		t: d'2020-01-01T00:00:00Z' + <duration> (<string> ($n % 50) + 'd'),
		du: <duration> (<string> ($n % 9) + 'h'),
		o: IF $n % 4 = 0 { NONE } ELSE { $n % 13 },
		nl: IF $n % 5 = 0 { NULL } ELSE { $n % 11 },
		tags: IF $n % 2 = 0 { ['a', 'b'] } ELSE { ['c'] },
		obj: { x: $n % 9, y: IF $n % 6 = 0 { NONE } ELSE { 'y' + <string> ($n % 3) } },
		flex: IF $n % 7 = 0 { { k: $n % 5 } } ELSE { {} }
	} RETURN NONE;
};
SELECT id, i FROM item ORDER BY i ASC LIMIT 5;
SELECT id, i FROM item ORDER BY i DESC LIMIT 5;
SELECT id, s FROM item ORDER BY s ASC LIMIT 5;
SELECT id, s FROM item ORDER BY s DESC LIMIT 5;
SELECT id, f FROM item ORDER BY f ASC LIMIT 5;
SELECT id, f FROM item ORDER BY f DESC LIMIT 5;
SELECT id, b FROM item ORDER BY b ASC LIMIT 5;
SELECT id, b FROM item ORDER BY b DESC LIMIT 5;
SELECT id, dec FROM item ORDER BY dec ASC LIMIT 5;
SELECT id, dec FROM item ORDER BY dec DESC LIMIT 5;
SELECT id, t FROM item ORDER BY t ASC LIMIT 5;
SELECT id, t FROM item ORDER BY t DESC LIMIT 5;
SELECT id, du FROM item ORDER BY du ASC LIMIT 5;
SELECT id, du FROM item ORDER BY du DESC LIMIT 5;
SELECT id, o FROM item ORDER BY o ASC LIMIT 5;
SELECT id, o FROM item ORDER BY o DESC LIMIT 5;
SELECT id, nl FROM item ORDER BY nl ASC LIMIT 5;
SELECT id, nl FROM item ORDER BY nl DESC LIMIT 5;
SELECT id, obj.x FROM item ORDER BY obj.x ASC LIMIT 5;
SELECT id, obj.x FROM item ORDER BY obj.x DESC LIMIT 5;
SELECT id, obj.y FROM item ORDER BY obj.y ASC LIMIT 5;
SELECT id, obj.y FROM item ORDER BY obj.y DESC LIMIT 5;
SELECT id, flex.k FROM item ORDER BY flex.k ASC LIMIT 5;
SELECT id, flex.k FROM item ORDER BY flex.k DESC LIMIT 5;
SELECT id, tags FROM item ORDER BY tags ASC LIMIT 5;
SELECT id, tags FROM item ORDER BY tags DESC LIMIT 5;
SELECT id, i FROM item ORDER BY i LIMIT 50;
SELECT id, i FROM item ORDER BY i DESC LIMIT 50 START 3;
SELECT id, i, s FROM item ORDER BY i, s LIMIT 5;
SELECT id, i, s FROM item ORDER BY s DESC, i LIMIT 5;
SELECT id, nl, i FROM item ORDER BY nl DESC, i LIMIT 5;
SELECT id, o, nl FROM item ORDER BY o, nl DESC LIMIT 7;
SELECT id, b, f FROM item ORDER BY b DESC, f LIMIT 7 START 2;
SELECT id, o FROM item WHERE o IS NONE ORDER BY i DESC LIMIT 5;
SELECT id, dec FROM item WHERE i > 10 AND b ORDER BY dec LIMIT 5;
SELECT id, t FROM item WHERE nl IS NULL ORDER BY t DESC, id LIMIT 5;
SELECT id, i FROM item ORDER BY i LIMIT 0;
SELECT id, i FROM item ORDER BY i LIMIT 2000 START 1195;
