/**
[test]
reason = "A parenthesised path in a stored expression keeps its parentheses: the catalog holds the expression as canonical SurrealQL text, so dropping them would make every row compute a different value"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ events: {  }, fields: { v: "DEFINE FIELD v ON t VALUE ([{ a: [1] }, { a: [2] }].a)[0] PERMISSIONS FULL" }, indexes: {  }, lives: {  }, tables: {  } }"""

[[test.results]]
value = "{ id: t:1, v: [1] }"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[1, 2]"
*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD v ON t VALUE ([{ a: [1] }, { a: [2] }].a)[0];
INFO FOR TABLE t;

CREATE ONLY t:1;

-- The two spellings the stored text must not be confused for one another:
-- the index applies to the result of the path, not to each element of it.
RETURN ([{ a: [1] }, { a: [2] }].a)[0];
RETURN [{ a: [1] }, { a: [2] }].a[0];
