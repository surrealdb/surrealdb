/**
[test]
reason = "Test query planner with multiple indexes and OR/AND conditions (new executor)"

[[test.results]]
value = "[{ company: 'SurrealDB', genre: 'm', id: person:tobie, name: 'Tobie' }]"

[[test.results]]
value = "[{ company: 'SurrealDB', genre: 'm', id: person:jaime, name: 'Jaime' }]"

[[test.results]]
value = "[{ company: 'SurrealDB', genre: 'f', id: person:lizzie, name: 'Lizzie' }]"

[[test.results]]
value = "[{ company: 'Metkayina', genre: 'f', id: person:neytiry, name: 'Neytiri' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 2}
    Filter [ctx: Db] [predicate: name = 'Jaime' OR genre = 'm'] {rows: 2}
        IndexScan [ctx: Db] [index: uniq_name, access: , direction: Forward] {rows: 4}

Total rows: 2"'''

[[test.results]]
value = "[{ name: 'Jaime' }, { name: 'Tobie' }]"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 1}
    Filter [ctx: Db] [predicate: name = 'Jaime' AND genre = 'm'] {rows: 1}
        IndexScan [ctx: Db] [index: uniq_name, access: = 'Jaime', direction: Forward] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "[{ name: 'Jaime' }]"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 3}
    Filter [ctx: Db] [predicate: name = 'Jaime' OR genre = 'm' OR company @@ 'surrealdb'] {rows: 3}
        IndexScan [ctx: Db] [index: uniq_name, access: , direction: Forward] {rows: 4}

Total rows: 3"'''

[[test.results]]
value = "[{ name: 'Jaime' }, { name: 'Lizzie' }, { name: 'Tobie' }]"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 1}
    Filter [ctx: Db] [predicate: name = 'Jaime' AND genre = 'm' AND company @@ 'surrealdb'] {rows: 1}
        IndexScan [ctx: Db] [index: uniq_name, access: = 'Jaime', direction: Forward] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "[{ name: 'Jaime' }]"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 2}
    SortByKey [ctx: Db] [sort_keys: name ASC] {rows: 2}
        TableScan [ctx: Db] [table: person, direction: Forward, predicate: name = 'Jaime' OR genre = 'm'] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[{ name: 'Jaime' }, { name: 'Tobie' }]"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 3}
    Filter [ctx: Db] [predicate: name = 'Jaime' OR genre = 'm' OR company @@ 'surrealdb'] {rows: 3}
        IndexScan [ctx: Db] [index: uniq_name, access: , direction: Forward] {rows: 4}

Total rows: 3"'''

[[test.results]]
value = "[{ name: 'Jaime' }, { name: 'Lizzie' }, { name: 'Tobie' }]"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 3}
    SortByKey [ctx: Db] [sort_keys: name ASC] {rows: 3}
        TableScan [ctx: Db] [table: person, direction: Forward, predicate: name = 'Jaime' OR genre = 'm' OR company @@ 'surrealdb'] {rows: 3}

Total rows: 3"'''

[[test.results]]
value = "[{ name: 'Jaime' }, { name: 'Lizzie' }, { name: 'Tobie' }]"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 1}
    SortByKey [ctx: Db] [sort_keys: name ASC] {rows: 1}
        Filter [ctx: Db] [predicate: name = 'Jaime' AND genre = 'm' AND company @@ 'surrealdb'] {rows: 1}
            FullTextScan [ctx: Db] [index: ft_company, query: surrealdb] {rows: 3}

Total rows: 1"'''

[[test.results]]
value = "[{ name: 'Jaime' }]"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 2}
    SortByKey [ctx: Db] [sort_keys: name ASC] {rows: 2}
        TableScan [ctx: Db] [table: person, direction: Forward, predicate: name = 'Jaime' OR genre = 'm'] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[{ name: 'Jaime' }, { name: 'Tobie' }]"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 1}
    SortByKey [ctx: Db] [sort_keys: name ASC] {rows: 1}
        TableScan [ctx: Db] [table: person, direction: Forward, predicate: name = 'Jaime' AND genre = 'm'] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "[{ name: 'Jaime' }]"

*/

CREATE person:tobie SET name = 'Tobie', genre='m', company='SurrealDB';
CREATE person:jaime SET name = 'Jaime', genre='m', company='SurrealDB';
CREATE person:lizzie SET name = 'Lizzie', genre='f', company='SurrealDB';
CREATE person:neytiry SET name = 'Neytiri', genre='f', company='Metkayina';
DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX ft_company ON person FIELDS company FULLTEXT ANALYZER simple BM25;
DEFINE INDEX uniq_name ON TABLE person COLUMNS name UNIQUE;
DEFINE INDEX idx_genre ON TABLE person COLUMNS genre;

-- Two-index OR
EXPLAIN ANALYZE SELECT name FROM person WHERE name = 'Jaime' OR genre = 'm' ORDER BY name;
SELECT name FROM person WHERE name = 'Jaime' OR genre = 'm' ORDER BY name;
-- Two-index AND
EXPLAIN ANALYZE SELECT name FROM person WHERE name = 'Jaime' AND genre = 'm' ORDER BY name;
SELECT name FROM person WHERE name = 'Jaime' AND genre = 'm' ORDER BY name;

-- Three-index OR
EXPLAIN ANALYZE SELECT name FROM person WHERE name = 'Jaime' OR genre = 'm' OR company @@ 'surrealdb' ORDER BY name;
SELECT name FROM person WHERE name = 'Jaime' OR genre = 'm' OR company @@ 'surrealdb' ORDER BY name;
-- Three-index AND
EXPLAIN ANALYZE SELECT name FROM person WHERE name = 'Jaime' AND genre = 'm' AND company @@ 'surrealdb' ORDER BY name;
SELECT name FROM person WHERE name = 'Jaime' AND genre = 'm' AND company @@ 'surrealdb' ORDER BY name;

-- WITH INDEX hint forcing specific indexes
EXPLAIN ANALYZE SELECT name FROM person WITH INDEX idx_genre WHERE name = 'Jaime' OR genre = 'm' ORDER BY name;
SELECT name FROM person WITH INDEX idx_genre WHERE name = 'Jaime' OR genre = 'm' ORDER BY name;

-- WITH INDEX all three explicitly
EXPLAIN ANALYZE SELECT name FROM person WITH INDEX uniq_name,idx_genre,ft_company WHERE name = 'Jaime' OR genre = 'm' OR company @@ 'surrealdb' ORDER BY name;
SELECT name FROM person WITH INDEX uniq_name,idx_genre,ft_company WHERE name = 'Jaime' OR genre = 'm' OR company @@ 'surrealdb' ORDER BY name;

-- WITH INDEX forcing only FT index for three-index query
EXPLAIN ANALYZE SELECT name FROM person WITH INDEX ft_company WHERE name = 'Jaime' OR genre = 'm' OR company @@ 'surrealdb' ORDER BY name;
SELECT name FROM person WITH INDEX ft_company WHERE name = 'Jaime' OR genre = 'm' OR company @@ 'surrealdb' ORDER BY name;
-- FT index AND
EXPLAIN ANALYZE SELECT name FROM person WITH INDEX ft_company WHERE name = 'Jaime' AND genre = 'm' AND company @@ 'surrealdb' ORDER BY name;
SELECT name FROM person WITH INDEX ft_company WHERE name = 'Jaime' AND genre = 'm' AND company @@ 'surrealdb' ORDER BY name;

-- WITH NOINDEX
EXPLAIN ANALYZE SELECT name FROM person WITH NOINDEX WHERE name = 'Jaime' OR genre = 'm' ORDER BY name;
SELECT name FROM person WITH NOINDEX WHERE name = 'Jaime' OR genre = 'm' ORDER BY name;
EXPLAIN ANALYZE SELECT name FROM person WITH NOINDEX WHERE name = 'Jaime' AND genre = 'm' ORDER BY name;
SELECT name FROM person WITH NOINDEX WHERE name = 'Jaime' AND genre = 'm' ORDER BY name;
