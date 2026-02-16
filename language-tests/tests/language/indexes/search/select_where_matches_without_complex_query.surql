/**
[env]
new-planner-strategy = "best-effort-ro"

[test]

[[test.results]]
value = "[{ content: 'fox jumped over the lazy dog', host: 'test', id: page:1, title: 'the quick brown' }]"

[[test.results]]
value = "[{ content: 'jumped over the lazy dog', host: 'test', id: page:2, title: 'the fast fox' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: page:1, sc1: 0f, sc2: 0f }, { id: page:2, sc1: 0f, sc2: 0f }]"

[[test.results]]
value = """{ children: [{ attributes: { predicate: "title @1@ 'dog' OR content @2@ 'dog'" }, children: [{ attributes: { access: "= 'test'", direction: 'Forward', index: 'page_host' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', expressions: [{ role: 'predicate', sql: "title @1@ 'dog' OR content @2@ 'dog'" }], operator: 'Filter' }], context: 'Db', expressions: [{ role: 'field', sql: 'id' }, { role: 'field', sql: 'search::score(...)' }, { role: 'field', sql: 'search::score(...)' }], operator: 'Project' }"""

[[test.results]]
value = "[{ id: page:1, sc1: 0f, sc2: 0f }, { id: page:2, sc1: 0f, sc2: 0f }]"

[[test.results]]
value = "[{ id: page:1, sc1: 0f, sc2: 0f }, { id: page:2, sc1: 0f, sc2: 0f }]"

*/
CREATE page:1 SET title = 'the quick brown', content = 'fox jumped over the lazy dog', host = 'test';
CREATE page:2 SET title = 'the fast fox', content = 'jumped over the lazy dog', host = 'test';
DEFINE ANALYZER simple TOKENIZERS blank,class;
DEFINE INDEX page_title ON page FIELDS title FULLTEXT ANALYZER simple BM25;
DEFINE INDEX page_content ON page FIELDS content FULLTEXT ANALYZER simple BM25;
DEFINE INDEX page_host ON page FIELDS host;
SELECT id, search::score(1) as sc1, search::score(2) as sc2
	FROM page WHERE (title @1@ 'dog' OR content @2@ 'dog');
SELECT id, search::score(1) as sc1, search::score(2) as sc2
	FROM page WHERE host = 'test'
	AND (title @1@ 'dog' OR content @2@ 'dog') explain;
SELECT id, search::score(1) as sc1, search::score(2) as sc2
	FROM page WHERE
	host = 'test'
	AND (title @1@ 'dog' OR content @2@ 'dog')
	ORDER BY id;
SELECT id, search::score(1) as sc1, search::score(2) as sc2
	FROM page WHERE
	host = 'test'
	AND (title @1@ 'dog' OR content @2@ 'dog')
	ORDER BY id;
