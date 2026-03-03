/**
[env]
new-planner-strategy = "compute-only"

[test]
reason = "Test query planner with multiple indexes and OR/AND conditions"

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
value = "[{ name: 'Jaime' }, { name: 'Tobie' }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'uniq_name', operator: '=', value: 'Jaime' }, table: 'person' }, operation: 'Iterate Index' }, { detail: { plan: { index: 'idx_genre', operator: '=', value: 'm' }, table: 'person' }, operation: 'Iterate Index' }, { detail: { type: 'MemoryOrdered' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 2 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ name: 'Jaime' }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx_genre', operator: '=', value: 'm' }, table: 'person' }, operation: 'Iterate Index' }, { detail: { type: 'MemoryOrdered' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 1 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ name: 'Jaime' }, { name: 'Lizzie' }, { name: 'Tobie' }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'uniq_name', operator: '=', value: 'Jaime' }, table: 'person' }, operation: 'Iterate Index' }, { detail: { plan: { index: 'idx_genre', operator: '=', value: 'm' }, table: 'person' }, operation: 'Iterate Index' }, { detail: { plan: { index: 'ft_company', operator: '@@', value: 'surrealdb' }, table: 'person' }, operation: 'Iterate Index' }, { detail: { type: 'MemoryOrdered' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 3 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ name: 'Jaime' }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'ft_company', operator: '@@', value: 'surrealdb' }, table: 'person' }, operation: 'Iterate Index' }, { detail: { type: 'MemoryOrdered' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 1 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ name: 'Jaime' }, { name: 'Tobie' }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'person' }, operation: 'Iterate Table' }, { detail: { type: 'MemoryOrdered' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 2 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ name: 'Jaime' }, { name: 'Lizzie' }, { name: 'Tobie' }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'uniq_name', operator: '=', value: 'Jaime' }, table: 'person' }, operation: 'Iterate Index' }, { detail: { plan: { index: 'idx_genre', operator: '=', value: 'm' }, table: 'person' }, operation: 'Iterate Index' }, { detail: { plan: { index: 'ft_company', operator: '@@', value: 'surrealdb' }, table: 'person' }, operation: 'Iterate Index' }, { detail: { type: 'MemoryOrdered' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 3 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ name: 'Jaime' }, { name: 'Lizzie' }, { name: 'Tobie' }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'person' }, operation: 'Iterate Table' }, { detail: { type: 'MemoryOrdered' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 3 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ name: 'Jaime' }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'ft_company', operator: '@@', value: 'surrealdb' }, table: 'person' }, operation: 'Iterate Index' }, { detail: { type: 'MemoryOrdered' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 1 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ name: 'Jaime' }, { name: 'Tobie' }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'person' }, operation: 'Iterate Table' }, { detail: { reason: 'WITH NOINDEX' }, operation: 'Fallback' }, { detail: { type: 'MemoryOrdered' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 2 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ name: 'Jaime' }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'person' }, operation: 'Iterate Table' }, { detail: { reason: 'WITH NOINDEX' }, operation: 'Fallback' }, { detail: { type: 'MemoryOrdered' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 1 }, operation: 'Fetch' }]"
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
SELECT name FROM person WHERE name = 'Jaime' OR genre = 'm' ORDER BY name;
SELECT name FROM person WHERE name = 'Jaime' OR genre = 'm' ORDER BY name EXPLAIN FULL;
-- Two-index AND
SELECT name FROM person WHERE name = 'Jaime' AND genre = 'm' ORDER BY name;
SELECT name FROM person WHERE name = 'Jaime' AND genre = 'm' ORDER BY name EXPLAIN FULL;

-- Three-index OR
SELECT name FROM person WHERE name = 'Jaime' OR genre = 'm' OR company @@ 'surrealdb' ORDER BY name;
SELECT name FROM person WHERE name = 'Jaime' OR genre = 'm' OR company @@ 'surrealdb' ORDER BY name EXPLAIN FULL;
-- Three-index AND
SELECT name FROM person WHERE name = 'Jaime' AND genre = 'm' AND company @@ 'surrealdb' ORDER BY name;
SELECT name FROM person WHERE name = 'Jaime' AND genre = 'm' AND company @@ 'surrealdb' ORDER BY name EXPLAIN FULL;

-- WITH INDEX hint forcing specific indexes
SELECT name FROM person WITH INDEX idx_genre WHERE name = 'Jaime' OR genre = 'm' ORDER BY name;
SELECT name FROM person WITH INDEX idx_genre WHERE name = 'Jaime' OR genre = 'm' ORDER BY name EXPLAIN FULL;

-- WITH INDEX all three explicitly
SELECT name FROM person WITH INDEX uniq_name,idx_genre,ft_company WHERE name = 'Jaime' OR genre = 'm' OR company @@ 'surrealdb' ORDER BY name;
SELECT name FROM person WITH INDEX uniq_name,idx_genre,ft_company WHERE name = 'Jaime' OR genre = 'm' OR company @@ 'surrealdb' ORDER BY name EXPLAIN FULL;

-- WITH INDEX forcing only FT index for three-index query
SELECT name FROM person WITH INDEX ft_company WHERE name = 'Jaime' OR genre = 'm' OR company @@ 'surrealdb' ORDER BY name;
SELECT name FROM person WITH INDEX ft_company WHERE name = 'Jaime' OR genre = 'm' OR company @@ 'surrealdb' ORDER BY name EXPLAIN FULL;
-- FT index AND
SELECT name FROM person WITH INDEX ft_company WHERE name = 'Jaime' AND genre = 'm' AND company @@ 'surrealdb' ORDER BY name;
SELECT name FROM person WITH INDEX ft_company WHERE name = 'Jaime' AND genre = 'm' AND company @@ 'surrealdb' ORDER BY name EXPLAIN FULL;

-- WITH NOINDEX
SELECT name FROM person WITH NOINDEX WHERE name = 'Jaime' OR genre = 'm' ORDER BY name;
SELECT name FROM person WITH NOINDEX WHERE name = 'Jaime' OR genre = 'm' ORDER BY name EXPLAIN FULL;
SELECT name FROM person WITH NOINDEX WHERE name = 'Jaime' AND genre = 'm' ORDER BY name;
SELECT name FROM person WITH NOINDEX WHERE name = 'Jaime' AND genre = 'm' ORDER BY name EXPLAIN FULL;
