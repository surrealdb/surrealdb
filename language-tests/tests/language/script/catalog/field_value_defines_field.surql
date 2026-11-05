/**
[test]
reason = "A field whose VALUE runs an embedded script defining another field of its table does not apply it to the write that runs it, and applies it to the next write."

[[test.results]]
error = "The VALUE clause must not have effects: a script, module or silo function runs code whose effects cannot be known where the clause is defined"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }]"

[[test.results]]
value = "[{ id: t:3 }]"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }, { id: t:3 }]"
*/
DEFINE FIELD g ON t VALUE function() { return surrealdb.query("DEFINE FIELD OVERWRITE f ON t VALUE 42") };
CREATE t:1, t:2;
CREATE t:3;
SELECT * FROM t;
