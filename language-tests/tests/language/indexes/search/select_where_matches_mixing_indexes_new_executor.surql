/**
[env]
planner-strategy = ["all-ro"]

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:1, name: 'Tobie', securityNumber: '123456' }]"

[[test.results]]
value = "[{ id: person:2, name: 'Jaime', securityNumber: 'ABCDEF' }]"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: *] {rows: 1}\n    Filter [ctx: Db] [predicate: name @@ 'Tobie' OR securityNumber == '123456'] {rows: 1}\n        BitmapResolve [ctx: Db] [table: person] {rows: 1}\n            BitmapOr [ctx: Db] {rows: 1}\n                BitmapFullTextScan [ctx: Db] [index: idxPersonName, query: Tobie] {rows: 1}\n                BitmapIndexScan [ctx: Db] [index: idxSecurityNumber, access: = '123456'] {rows: 1}\n\nTotal rows: 1"'''

[[test.results]]
value = "[{ id: person:1, name: 'Tobie', securityNumber: '123456' }]"

*/
DEFINE TABLE person;
DEFINE ANALYZER edgeNgram TOKENIZERS class FILTERS lowercase,ascii,edgeNgram(1,10);
DEFINE INDEX idxPersonName ON TABLE person COLUMNS name FULLTEXT ANALYZER edgeNgram BM25;
DEFINE INDEX idxSecurityNumber ON TABLE person COLUMNS securityNumber UNIQUE;
CREATE person:1 content {name: 'Tobie', securityNumber: '123456'};
CREATE person:2 content {name: 'Jaime', securityNumber: 'ABCDEF'};
EXPLAIN ANALYZE SELECT * FROM person WHERE name @@ 'Tobie' || securityNumber == '123456';
SELECT * FROM person WHERE name @@ 'Tobie' || securityNumber == '123456';
