/**
[test]

[[test.results]]
value = "[{ genre: 'm', id: person:tobie, name: 'Tobie' }]"

[[test.results]]
value = "[{ genre: 'm', id: person:jaime, name: 'Jaime' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ detail: { plan: { index: 'ft_name', operator: '@@', value: 'Jaime' }, table: 'person' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ name: 'Jaime' }]"

*/
CREATE person:tobie SET name = 'Tobie', genre='m';
CREATE person:jaime SET name = 'Jaime', genre='m';
DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX ft_name ON TABLE person COLUMNS name SEARCH ANALYZER simple BM25(1.2,0.75);
SELECT name FROM person WHERE name @@ 'Jaime' AND genre = 'm' EXPLAIN;
SELECT name FROM person WHERE name @@ 'Jaime' AND genre = 'm';
