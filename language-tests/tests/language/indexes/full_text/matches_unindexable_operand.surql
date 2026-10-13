/**
[env]
namespace = true
database = true

[test]
reason = """
MATCHES shapes that cannot name a full-text index, identical across the legacy
and streaming executors (this file deliberately runs under every planner
strategy as a differential proof).

Two operand shapes carry no index probe: a left operand that is not an idiom,
and a right operand that does not resolve to a value at plan time. Both follow
the same per-row decision as an unregistered MATCHES — `false` for a row no
query executor would have seen (no document at all, or a record-id source),
'no suitable index' for a row of a full table source. Neither may answer `true`
without consulting an index.

A right operand that is a non-string value still names a probe: the query string
is the value's raw rendering, so `title @@ 42` searches the analyzed term `42`
and matches only the rows containing it.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: doc:1, other: 'hello', title: 'Hello World' }]"

[[test.results]]
value = "[{ id: doc:2, other: 'answer', title: 'Answer 42' }]"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
error = "There was no suitable index supporting the expression: 'abc' @@ 'zzz'"

[[test.results]]
error = "There was no suitable index supporting the expression: ['hello'] @@ 'hello'"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: doc:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: doc:2 }]"

[[test.results]]
error = "There was no suitable index supporting the expression: title @@ other"

[[test.results]]
value = "[{ id: doc:1 }]"

[[test.results]]
error = "There was no suitable index supporting the expression: other @@ 'hello'"
*/

DEFINE ANALYZER az TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX ft_title ON doc COLUMNS title FULLTEXT ANALYZER az BM25;

CREATE doc:1 SET title = 'Hello World', other = 'hello';
CREATE doc:2 SET title = 'Answer 42', other = 'answer';

-- No document at all: no query executor, so no match rather than a match.
RETURN 'abc' @@ 'zzz';
RETURN 42 @@ 'zzz';

-- A non-idiom left operand over a full table source names no index.
SELECT id FROM doc WHERE 'abc' @@ 'zzz';
SELECT id FROM doc WHERE ['hello'] @@ 'hello';

-- A record-id source never gets a query executor, so the same expression is
-- false there instead of erroring.
SELECT id FROM doc:1 WHERE 'abc' @@ 'zzz';

-- A right operand that resolves to a non-string value at plan time still
-- probes the index, searching for that value's raw rendering.
SELECT id FROM doc WHERE title @@ 42;
LET $n = 42;
SELECT id FROM doc WHERE title @@ $n;

-- A right operand that cannot resolve at plan time names no index.
SELECT id FROM doc WHERE title @@ other;

-- An indexed field with a plain string query is unaffected.
SELECT id FROM doc WHERE title @@ 'hello';

-- A field with no full-text index names no index whichever side is which.
SELECT id FROM doc WHERE other @@ 'hello';
