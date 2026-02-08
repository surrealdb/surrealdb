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
value = '''"Project [ctx: Db]\n└────> SortByKey [ctx: Db] [sort_keys: name ASC]\n       └────> Scan [ctx: Db] [source: person]\n"'''

[[test.results]]
value = "[{ name: 'Jaime' }, { name: 'Tobie' }]"

[[test.results]]
value = "[{ name: 'Tobie' }]"

*/
CREATE person:tobie SET name = 'Tobie', genre='m';
CREATE person:jaime SET name = 'Jaime', genre='m';
DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX ft_name ON TABLE person COLUMNS name FULLTEXT ANALYZER simple BM25(1.2,0.75);
EXPLAIN SELECT name FROM person ORDER BY name ASC;
SELECT name FROM person ORDER BY name ASC;
SELECT name FROM person WHERE name @@ 'tobie' ORDER BY name;
