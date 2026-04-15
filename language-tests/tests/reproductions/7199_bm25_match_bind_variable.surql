/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "BM25 @N@ match operator must return results when the RHS is a bind variable"
issue = 7199

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: book:1, title: 'Hello World' }]"

[[test.results]]
value = "[{ id: book:1, title: 'Hello World' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: book:1, title: 'Hello World' }]"

[[test.results]]
value = "[{ id: book:1, title: 'Hello World' }]"
*/

DEFINE ANALYZER simple TOKENIZERS blank,class;
DEFINE INDEX title_ft ON book FIELDS title FULLTEXT ANALYZER simple BM25;
CREATE book:1 SET title = 'Hello World';

-- Literal string: should return book:1
SELECT * FROM book WHERE title @1@ 'Hello';

-- Bind variable with the same value: should also return book:1
LET $q = 'Hello';
SELECT * FROM book WHERE title @1@ $q;

-- Also test the @@ operator with bind variable
SELECT * FROM book WHERE title @@ $q;
