/**
[env]
namespace = true
database = true

[test]
reason = "A MATCHES a write evaluates is judged as main judges it: answered from the full-text index for the records of a table the statement names whole, a type::table call's included; judged on its own value for a record a record id or type::record names; refused outside the WHERE over a table read whole; and an index hint, a residual conjunct or an unbound query parameter leave the result as a read gives it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ body: 'hello world', id: doc:1, y: 1 }]"

[[test.results]]
value = "[{ body: 'bye', id: doc:2, y: 2 }]"

[[test.results]]
error = "There was no suitable index supporting the expression: body @@ 'hello'"

[[test.results]]
error = "There was no suitable index supporting the expression: body @@ 'hello'"

[[test.results]]
value = "['<b>hello</b> world']"

[[test.results]]
value = "[doc:1]"

[[test.results]]
value = "[doc:1]"

[[test.results]]
value = "[doc:1, doc:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[doc:1]"

[[test.results]]
value = "[doc:1]"

[[test.results]]
value = "[doc:1]"

[[test.results]]
error = "There was no suitable index supporting the expression: body @@ 'hello'"

[[test.results]]
value = "[doc:1]"

[[test.results]]
value = "[doc:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[false]"

[[test.results]]
value = "[false]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[]"

*/
DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX ft ON doc FIELDS body FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
CREATE doc:1 SET body = 'hello world', y = 1;
CREATE doc:2 SET body = 'bye', y = 2;
UPDATE doc SET x = body @@ 'hello' WHERE y = 1 RETURN VALUE x;
UPDATE doc SET x = body @@ 'hello' RETURN VALUE x;
UPDATE doc SET h = search::highlight('<b>', '</b>', 1) WHERE body @1@ 'hello' RETURN VALUE h;
UPDATE doc:1 SET q = 1 WHERE body @@ 'hello' RETURN VALUE id;
UPSERT doc:1 SET q = 2 WHERE body @@ 'hello' RETURN VALUE id;
UPDATE doc, doc:1 SET r = 1 WHERE body @@ 'hello' RETURN VALUE id;
LET $t = 'doc';
UPDATE type::table($t) SET s = 1 WHERE body @@ 'hello' RETURN VALUE id;
UPDATE type::table('doc') SET s = 2 WHERE body @@ 'hello' RETURN VALUE id;
UPDATE type::record('doc', 1) SET s = 3 WHERE body @@ 'hello' RETURN VALUE id;
UPDATE type::table('doc') SET x = body @@ 'hello' RETURN VALUE x;
UPDATE doc WITH INDEX ft SET w = 1 WHERE body @@ 'hello' RETURN VALUE id;
UPDATE doc WITH NOINDEX SET w = 2 WHERE body @@ 'hello' RETURN VALUE id;
UPDATE doc SET v = 1 WHERE body @@ 'hello' AND y = 2 RETURN VALUE id;
UPDATE doc SET k = 1 WHERE body @@ $missing RETURN VALUE id;
CREATE doc SET x = body @@ 'a', body = 'a' RETURN VALUE x;
UPSERT doc:1 SET x = body @@ 'hello' RETURN VALUE x;
UPSERT doc SET z = 1 WHERE body @@ 'nothere' RETURN VALUE z;
DELETE doc WHERE body @@ 'zzz' RETURN BEFORE;
