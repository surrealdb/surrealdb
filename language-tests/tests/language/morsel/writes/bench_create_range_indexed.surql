/**
[env]
namespace = true
database = true
timeout = 30000

[test]
reason = "the statement of the bench create_range_100k_indexed over a few thousand ids into a table with an index answers as the serial run does, whatever width its compute half runs at"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 2999 }]"

[[test.results]]
value = "[{ id: record:1, n: 1, name: 'n1', score: 0.5f, tags: ['a', 'b', 'c'] }, { id: record:1500, n: 1500, name: 'n1500', score: 750f, tags: ['a', 'b', 'c'] }, { id: record:2999, n: 2999, name: 'n2999', score: 1499.5f, tags: ['a', 'b', 'c'] }]"

[[test.results]]
value = "[2990, 2991, 2992, 2993, 2994, 2995, 2996, 2997, 2998, 2999]"

*/

DEFINE TABLE record SCHEMAFULL;
DEFINE FIELD n ON record TYPE int VALUE record::id(id);
DEFINE FIELD name ON record TYPE string VALUE 'n' + <string> record::id(id);
DEFINE FIELD score ON record TYPE float VALUE record::id(id) * 0.5;
DEFINE FIELD tags ON record TYPE array<string>;
DEFINE INDEX by_n ON record FIELDS n;
CREATE |record:1..3000| SET tags = ['a', 'b', 'c'] RETURN NONE;
SELECT count() FROM record GROUP ALL;
SELECT * FROM record WHERE id IN [record:1, record:1500, record:2999] ORDER BY id;
SELECT VALUE n FROM record WITH INDEX by_n WHERE n >= 2990 ORDER BY n;
