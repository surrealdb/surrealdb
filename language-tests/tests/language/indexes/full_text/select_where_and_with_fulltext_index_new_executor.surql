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
value = '''"Project [ctx: Db] {rows: 1}
  Filter [ctx: Db] [predicate: name @@ 'Jaime' AND genre = 'm'] {rows: 1}
    FullTextScan [ctx: Db] [index: ft_name, query: Jaime] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "[{ name: 'Jaime' }]"

*/
CREATE person:tobie SET name = 'Tobie', genre='m';
CREATE person:jaime SET name = 'Jaime', genre='m';
DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX ft_name ON TABLE person COLUMNS name FULLTEXT ANALYZER simple BM25(1.2,0.75);
EXPLAIN ANALYZE SELECT name FROM person WHERE name @@ 'Jaime' AND genre = 'm';
SELECT name FROM person WHERE name @@ 'Jaime' AND genre = 'm';
