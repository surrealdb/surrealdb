/**
[env]
imports = ["language/graph/frontier_windows_permissions_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:1" }

[test]
reason = "SECURITY: a traversal over many sources or rows that reads the adjacency of its sources together must judge each record it reaches by the permissions of the caller exactly as the reads one at a time do: the owner's writes materialised the inline caches with every edge and reference, and the keys the reads take hold every edge, so a hidden vertex, a non-public edge or comment, and a field the caller may not select must stay out of the answers of every shape of path"

[[test.results]]
value = "[{ f: [person:7], id: person:1, secret: 's1' }, { f: [person:11, person:13], id: person:2, secret: NONE }, { f: [person:17, person:19], id: person:3, secret: NONE }, { f: [person:21], id: person:4, secret: NONE }, { f: [person:7, person:9], id: person:6, secret: NONE }, { f: [person:13], id: person:7, secret: NONE }, { f: [person:17], id: person:8, secret: NONE }, { f: [person:23], id: person:9, secret: NONE }, { f: [person:9, person:11], id: person:11, secret: NONE }, { f: [person:13], id: person:12, secret: NONE }, { f: [person:19], id: person:13, secret: NONE }, { f: [person:23, person:1], id: person:14, secret: NONE }, { f: [person:9], id: person:16, secret: NONE }, { f: [], id: person:17, secret: NONE }, { f: [person:19, person:21], id: person:18, secret: NONE }, { f: [person:1, person:3], id: person:19, secret: NONE }, { f: [person:11], id: person:21, secret: NONE }, { f: [person:17], id: person:22, secret: NONE }, { f: [person:21, person:23], id: person:23, secret: NONE }, { f: [person:1], id: person:24, secret: NONE }]"

[[test.results]]
value = "[{ edges: [knows:11], id: person:1 }, { edges: [knows:20, knows:22], id: person:2 }, { edges: [knows:31, knows:33], id: person:3 }, { edges: [knows:40], id: person:4 }, { edges: [knows:60, knows:62], id: person:6 }, { edges: [knows:71, knows:73], id: person:7 }, { edges: [knows:80], id: person:8 }, { edges: [knows:91], id: person:9 }, { edges: [knows:111, knows:113], id: person:11 }, { edges: [knows:120], id: person:12 }, { edges: [knows:131], id: person:13 }, { edges: [knows:140, knows:142], id: person:14 }, { edges: [knows:160], id: person:16 }, { edges: [knows:171], id: person:17 }, { edges: [knows:180, knows:182], id: person:18 }, { edges: [knows:191, knows:193], id: person:19 }, { edges: [knows:211], id: person:21 }, { edges: [knows:220, knows:222], id: person:22 }, { edges: [knows:231, knows:233], id: person:23 }, { edges: [knows:240], id: person:24 }]"

[[test.results]]
value = "[{ fof: [person:13], id: person:1 }, { fof: [person:9, person:11, person:19], id: person:2 }, { fof: [person:1, person:3], id: person:3 }, { fof: [person:11], id: person:4 }, { fof: [person:13, person:23], id: person:6 }, { fof: [person:19], id: person:7 }, { fof: [], id: person:8 }, { fof: [person:21, person:23], id: person:9 }, { fof: [person:23, person:9, person:11], id: person:11 }, { fof: [person:19], id: person:12 }, { fof: [person:1, person:3], id: person:13 }, { fof: [person:21, person:23, person:7], id: person:14 }, { fof: [person:23], id: person:16 }, { fof: [], id: person:17 }, { fof: [person:1, person:3, person:11], id: person:18 }, { fof: [person:7, person:17, person:19], id: person:19 }, { fof: [person:9, person:11], id: person:21 }, { fof: [], id: person:22 }, { fof: [person:11, person:21, person:23], id: person:23 }, { fof: [person:7], id: person:24 }]"

[[test.results]]
value = "[{ b: [person:14, person:19, person:24], id: person:1 }, { b: [], id: person:2 }, { b: [person:19], id: person:3 }, { b: [], id: person:4 }, { b: [], id: person:6 }, { b: [person:1, person:6], id: person:7 }, { b: [], id: person:8 }, { b: [person:6, person:11, person:16], id: person:9 }, { b: [person:2, person:11, person:21], id: person:11 }, { b: [], id: person:12 }, { b: [person:2, person:7, person:12], id: person:13 }, { b: [], id: person:14 }, { b: [], id: person:16 }, { b: [person:3, person:8, person:22], id: person:17 }, { b: [], id: person:18 }, { b: [person:3, person:13, person:18], id: person:19 }, { b: [person:4, person:18, person:23], id: person:21 }, { b: [], id: person:22 }, { b: [person:9, person:14, person:23], id: person:23 }, { b: [], id: person:24 }]"

[[test.results]]
value = "[{ c: [comment:10], id: person:1 }, { c: [comment:21], id: person:2 }, { c: [comment:30], id: person:3 }, { c: [comment:41], id: person:4 }, { c: [], id: person:6 }, { c: [comment:70], id: person:7 }, { c: [comment:81], id: person:8 }, { c: [comment:90], id: person:9 }, { c: [comment:110, comment:112], id: person:11 }, { c: [], id: person:12 }, { c: [comment:130], id: person:13 }, { c: [comment:141], id: person:14 }, { c: [comment:161], id: person:16 }, { c: [comment:170, comment:172], id: person:17 }, { c: [], id: person:18 }, { c: [comment:190], id: person:19 }, { c: [comment:210], id: person:21 }, { c: [comment:221], id: person:22 }, { c: [comment:230, comment:232], id: person:23 }, { c: [], id: person:24 }]"

[[test.results]]
value = "[{ id: person:1, reached: [comment:70] }, { id: person:2, reached: [comment:110, comment:112, comment:130] }, { id: person:3, reached: [comment:170, comment:172, comment:190] }, { id: person:4, reached: [comment:210] }, { id: person:6, reached: [comment:70, comment:90] }, { id: person:7, reached: [comment:130] }, { id: person:8, reached: [comment:170, comment:172] }, { id: person:9, reached: [comment:230, comment:232] }, { id: person:11, reached: [comment:90, comment:110, comment:112] }, { id: person:12, reached: [comment:130] }, { id: person:13, reached: [comment:190] }, { id: person:14, reached: [comment:230, comment:232, comment:10] }, { id: person:16, reached: [comment:90] }, { id: person:17, reached: [] }, { id: person:18, reached: [comment:190, comment:210] }, { id: person:19, reached: [comment:10, comment:30] }, { id: person:21, reached: [comment:110, comment:112] }, { id: person:22, reached: [comment:170, comment:172] }, { id: person:23, reached: [comment:210, comment:230, comment:232] }, { id: person:24, reached: [comment:10] }]"

[[test.results]]
value = "[{ id: person:1, secrets: [NONE] }, { id: person:2, secrets: [NONE, NONE] }, { id: person:3, secrets: [NONE, NONE] }, { id: person:4, secrets: [NONE] }, { id: person:6, secrets: [NONE, NONE] }, { id: person:7, secrets: [NONE] }, { id: person:8, secrets: [NONE] }, { id: person:9, secrets: [NONE] }, { id: person:11, secrets: [NONE, NONE] }, { id: person:12, secrets: [NONE] }, { id: person:13, secrets: [NONE] }, { id: person:14, secrets: [NONE, 's1'] }, { id: person:16, secrets: [NONE] }, { id: person:17, secrets: [] }, { id: person:18, secrets: [NONE, NONE] }, { id: person:19, secrets: ['s1', NONE] }, { id: person:21, secrets: [NONE] }, { id: person:22, secrets: [NONE] }, { id: person:23, secrets: [NONE, NONE] }, { id: person:24, secrets: ['s1'] }]"

[[test.results]]
value = "[{ id: person:1, ns: [7] }, { id: person:2, ns: [11, 13] }, { id: person:3, ns: [17, 19] }, { id: person:4, ns: [21] }, { id: person:6, ns: [7, 9] }, { id: person:7, ns: [13] }, { id: person:8, ns: [17] }, { id: person:9, ns: [23] }, { id: person:11, ns: [9, 11] }, { id: person:12, ns: [13] }, { id: person:13, ns: [19] }, { id: person:14, ns: [23, 1] }, { id: person:16, ns: [9] }, { id: person:17, ns: [] }, { id: person:18, ns: [19, 21] }, { id: person:19, ns: [1, 3] }, { id: person:21, ns: [11] }, { id: person:22, ns: [17] }, { id: person:23, ns: [21, 23] }, { id: person:24, ns: [1] }]"

[[test.results]]
value = "[[person:7], [person:11, person:13], [person:17, person:19], [person:21], [person:7, person:9], [person:13], [person:17], [person:23], [person:9, person:11], [person:13], [person:19], [person:23, person:1], [person:9], [], [person:19, person:21], [person:1, person:3], [person:11], [person:17], [person:21, person:23], [person:1]]"

[[test.results]]
value = "[[person:7], [person:11, person:13], [person:17, person:19], [person:21], [person:7, person:9], [person:13], [person:17], [person:23], [person:9, person:11], [person:13], [person:19], [person:23, person:1], [person:9], [], [person:19, person:21], [person:1, person:3], [person:11], [person:17], [person:21, person:23], [person:1]]"

[[test.results]]
value = "[comment:10, comment:21, comment:30, comment:41, comment:70, comment:81, comment:90, comment:110, comment:112, comment:130, comment:141, comment:161, comment:170, comment:172, comment:190, comment:210, comment:221, comment:230, comment:232]"

[[test.results]]
value = "[{ f: [person:21], id: person:4 }, { f: [person:7, person:9], id: person:6 }, { f: [person:13], id: person:7 }, { f: [person:17], id: person:8 }, { f: [person:23], id: person:9 }, { f: [person:9, person:11], id: person:11 }]"
*/

SELECT id, secret, ->knows->person AS f FROM person;
SELECT id, ->knows AS edges FROM person;
SELECT id, ->knows->person->knows->person AS fof FROM person;
SELECT id, <-knows<-person AS b FROM person;
SELECT id, <~comment AS c FROM person;
SELECT id, ->knows->person<~comment AS reached FROM person;
SELECT id, ->knows->person.secret AS secrets FROM person;
SELECT id, ->knows->person.n AS ns FROM person;
SELECT VALUE ->knows->person FROM person:1..=24;
SELECT VALUE (SELECT VALUE id FROM person)->knows->person FROM ONLY person:1;
SELECT VALUE (SELECT VALUE id FROM person)<~comment FROM ONLY person:1;
SELECT id, ->knows->person AS f FROM person LIMIT 6 START 3;
