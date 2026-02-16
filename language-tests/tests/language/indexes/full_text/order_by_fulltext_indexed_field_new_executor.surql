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
value = ''''Project [ctx: Db] {rows: 2}
    SortByKey [ctx: Db] [sort_keys: name ASC] {rows: 2}
        TableScan [ctx: Db] [table: person, direction: Forward] {rows: 2}

Total rows: 2''''

[[test.results]]
value = "[{ name: 'Jaime' }, { name: 'Tobie' }]"

[[test.results]]
value = "[{ name: 'Tobie' }]"

*/
CREATE person:tobie SET name = 'Tobie', genre='m';
CREATE person:jaime SET name = 'Jaime', genre='m';
DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX ft_name ON TABLE person COLUMNS name FULLTEXT ANALYZER simple BM25(1.2,0.75);
EXPLAIN ANALYZE SELECT name FROM person ORDER BY name ASC;
SELECT name FROM person ORDER BY name ASC;
SELECT name FROM person WHERE name @@ 'tobie' ORDER BY name;
