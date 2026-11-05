/**
[test]
reason = """
	A parameter holding a list of record ids, read as a whole FROM source, is
	judged by a full-text MATCHES as the same list written in FROM is, with and
	without a full-text index on the table, alone and beside a record id source.
	A list of ids with compound keys reads its records in list order, and so
	does a statement that reads a list of known ids beside a parameter.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: book:1, title: 'rust book' }]"

[[test.results]]
value = "[{ id: book:2, title: 'go book' }]"

[[test.results]]
value = "[{ id: book:[1, 'a'], title: 'rust array' }]"

[[test.results]]
value = "[{ id: book:{ k: 2 }, title: 'go object' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ m: false }, { m: false }]"

[[test.results]]
value = "[{ m: false }, { m: false }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ m: true }, { m: false }]"

[[test.results]]
value = "[{ m: true }, { m: false }]"

[[test.results]]
value = "[{ id: book:1, title: 'rust book' }]"

[[test.results]]
value = "[{ id: book:1, title: 'rust book' }]"

[[test.results]]
value = "[{ id: book:1, title: 'rust book' }, { id: book:1, title: 'rust book' }]"

[[test.results]]
value = "['go object', 'rust array', 'go book']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['go object', 'rust array', 'go book']"

[[test.results]]
value = "['go book', 'rust book', 'go object', 'rust array', 'go book']"
*/

DEFINE ANALYZER simple TOKENIZERS blank;
DEFINE TABLE book SCHEMALESS;
CREATE book:1 SET title = 'rust book';
CREATE book:2 SET title = 'go book';
CREATE book:[1, 'a'] SET title = 'rust array';
CREATE book:{ k: 2 } SET title = 'go object';
LET $b = [book:1, book:2];
SELECT title @@ 'rust' AS m FROM $b;
SELECT title @@ 'rust' AS m FROM [book:1, book:2];
SELECT * FROM $b WHERE title @@ 'rust';
SELECT * FROM [book:1, book:2] WHERE title @@ 'rust';
SELECT * FROM book:1, $b WHERE title @@ 'rust';
DEFINE INDEX ft ON book FIELDS title FULLTEXT ANALYZER simple BM25;
SELECT title @@ 'rust' AS m FROM $b;
SELECT title @@ 'rust' AS m FROM [book:1, book:2];
SELECT * FROM $b WHERE title @@ 'rust';
SELECT * FROM [book:1, book:2] WHERE title @@ 'rust';
SELECT * FROM book:1, $b WHERE title @@ 'rust';
SELECT VALUE title FROM [book:{ k: 2 }, book:[1, 'a'], book:[9, 'z'], book:2];
LET $c = [book:{ k: 2 }, book:[1, 'a'], book:2];
SELECT VALUE title FROM $c;
SELECT VALUE title FROM [book:2, book:1], $c;
