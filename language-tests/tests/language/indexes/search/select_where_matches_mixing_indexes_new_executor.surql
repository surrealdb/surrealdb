/**
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
value = '''"Project [ctx: Db] {rows: 1}\n└────> Filter [ctx: Db] [predicate: name @@ 'Tobie' OR securityNumber == '123456'] {rows: 1}\n       └────> UnionIndexScan [ctx: Db] [branches: 2] {rows: 1}\n              ├────> FullTextScan [ctx: Db] [index: idxPersonName, query: Tobie] {rows: 1}\n              └────> IndexScan [ctx: Db] [index: idxSecurityNumber, access: = '123456', direction: Forward] {rows: 1}\n\nTotal rows: 1"'''

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
