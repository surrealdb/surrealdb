/**
[env]
namespace = true
database = true

[test]
reason = "The reported defect is in the `surrealdb-parser` crate's `PeekableLexer::peek`, which no longer exists in this tree: statements are parsed by `surrealdb-parser-next`. This file runs statements that need two, three and four tokens of lookahead in a row, which is the shape that read an uninitialised ring-buffer slot, and pins that they parse and run."
issue = 7545

[[test.results]]
value = "[{ id: a:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"
*/

CREATE a:1;
SELECT id FROM a:1->b->c;
RETURN a:1 IS NOT NONE AND a:1.id = a:1;
