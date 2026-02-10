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
value = "[]"

[[test.results]]
value = "[{ name: 'Jaime' }]"

*/
DEFINE TABLE person;
DEFINE ANALYZER edgeNgram TOKENIZERS class FILTERS lowercase,ascii,edgeNgram(1,10);
DEFINE INDEX idxPersonName ON TABLE person COLUMNS name FULLTEXT ANALYZER edgeNgram BM25;
DEFINE INDEX idxSecurityNumber ON TABLE person COLUMNS securityNumber UNIQUE;
CREATE person:1 content {name: 'Tobie', securityNumber: '123456'};
CREATE person:2 content {name: 'Jaime', securityNumber: 'ABCDEF'};
EXPLAIN SELECT * FROM person WHERE name @@ 'Tobie' || securityNumber == '123456';
SELECT * FROM person WHERE name @@ 'Tobie' || securityNumber == '123456';
