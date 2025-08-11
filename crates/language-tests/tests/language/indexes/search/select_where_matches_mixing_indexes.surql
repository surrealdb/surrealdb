/**
[test]

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
value = "[{ detail: { plan: { index: 'idxPersonName', operator: '@@', value: 'Tobie' }, table: 'person' }, operation: 'Iterate Index' }, { detail: { plan: { index: 'idxSecurityNumber', operator: '=', value: '123456' }, table: 'person' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: person:1, name: 'Tobie', securityNumber: '123456' }]"

*/
DEFINE ANALYZER edgeNgram TOKENIZERS class FILTERS lowercase,ascii,edgeNgram(1,10);
DEFINE INDEX idxPersonName ON TABLE person COLUMNS name SEARCH ANALYZER edgeNgram BM25;
DEFINE INDEX idxSecurityNumber ON TABLE person COLUMNS securityNumber UNIQUE;
CREATE person:1 content {name: 'Tobie', securityNumber: '123456'};
CREATE person:2 content {name: 'Jaime', securityNumber: 'ABCDEF'};
SELECT * FROM person WHERE name @@ 'Tobie' || securityNumber == '123456' EXPLAIN;
SELECT * FROM person WHERE name @@ 'Tobie' || securityNumber == '123456';
