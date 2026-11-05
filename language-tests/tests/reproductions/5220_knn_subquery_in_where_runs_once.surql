/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "An uncorrelated subquery ranked by a nearest-neighbour search on the right of IN runs once for the whole statement rather than once per outer row, and its ids then read the outer table by key once bound to a parameter; both return the rows a brute-force search over the scanned table returns. A method call such as `guid.tb()` can call a closure field of an object, so a subquery holding one is evaluated per row; `record::tb(guid)` names the builtin."
issue = 5220

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[card:0, card:1, card:2]"

[[test.results]]
value = """{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'id INSIDE (subquery)' }, children: [{ attributes: { direction: 'Forward', table: 'card' }, context: 'Db', operator: 'TableScan' }, { attributes: { strategy: 'once' }, children: [{ attributes: { expr: 'guid' }, children: [{ attributes: { predicate: "vector != NONE AND record::tb(guid) = 'card'" }, children: [{ attributes: { dimension: '2', ef: '40', index: 'hnsw', k: '3', predicate: 'expr.short_circuit' }, context: 'Db', operator: 'KnnScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }], context: 'Db', operator: 'Subquery' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"""

[[test.results]]
value = "[card:0, card:1, card:2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[card:0, card:1, card:2]"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { record_id: '[card:0, card:1, card:2]' }, context: 'Db', operator: 'RecordIdScan' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "[card:0, card:1, card:2]"

[[test.results]]
value = """{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'id INSIDE (subquery)' }, children: [{ attributes: { direction: 'Forward', table: 'card' }, context: 'Db', operator: 'TableScan' }, { attributes: { strategy: 'apply' }, children: [{ attributes: { expr: 'guid' }, children: [{ attributes: { predicate: "guid.tb() = 'card'" }, children: [{ attributes: { dimension: '2', ef: '40', index: 'hnsw', k: '3', predicate: "guid.tb() = 'card'" }, context: 'Db', operator: 'KnnScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }], context: 'Db', operator: 'Subquery' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"""
*/

DEFINE TABLE card SCHEMALESS;
DEFINE TABLE embedding SCHEMALESS;
DEFINE INDEX hnsw ON embedding FIELDS vector HNSW DIMENSION 2 DIST EUCLIDEAN;
FOR $i IN 0..8 {
	CREATE type::record('card', $i) SET n = $i;
	CREATE type::record('embedding', $i) SET guid = type::record('card', $i), vector = [$i, $i];
};

SELECT VALUE id FROM card WHERE id INSIDE (SELECT VALUE guid FROM embedding WHERE vector != NONE AND record::tb(guid) = 'card' AND vector <|3,40|> [0, 0]);
SELECT VALUE id FROM card WHERE id INSIDE (SELECT VALUE guid FROM embedding WHERE vector != NONE AND record::tb(guid) = 'card' AND vector <|3,40|> [0, 0]) EXPLAIN;
SELECT VALUE id FROM card WITH NOINDEX WHERE id INSIDE (SELECT VALUE guid FROM embedding WITH NOINDEX WHERE vector <|3, EUCLIDEAN|> [0, 0]);

LET $guids = (SELECT VALUE guid FROM embedding WHERE vector != NONE AND record::tb(guid) = 'card' AND vector <|3,40|> [0, 0]);
SELECT VALUE id FROM card WHERE id INSIDE $guids;
SELECT VALUE id FROM card WHERE id INSIDE $guids EXPLAIN;

SELECT VALUE id FROM card WHERE id INSIDE (SELECT VALUE guid FROM embedding WHERE guid.tb() = 'card' AND vector <|3,40|> [0, 0]);
SELECT VALUE id FROM card WHERE id INSIDE (SELECT VALUE guid FROM embedding WHERE guid.tb() = 'card' AND vector <|3,40|> [0, 0]) EXPLAIN;
