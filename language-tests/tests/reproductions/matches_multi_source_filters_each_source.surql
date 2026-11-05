/**
[env]
namespace = true
database = true

[test]
reason = """
A MATCHES predicate over several FROM sources is judged for the rows of
every source, not only for those a full-text scan yields: a record read
beside the scan is kept only where it matches, and a source with no
full-text index raises the error a MATCHES without an index raises.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ content: 'hello world', id: doc:1 }, { content: 'goodbye', id: doc:2 }, { content: 'hello again', id: doc:3 }]"

[[test.results]]
value = "[{ content: 'hello there', id: other:1 }, { content: 'nothing', id: other:2 }]"

[[test.results]]
value = "[{ id: doc:1 }, { id: doc:3 }]"

[[test.results]]
value = "[{ id: doc:1 }, { id: doc:3 }, { id: doc:1 }]"

[[test.results]]
value = "[{ id: doc:1 }, { id: doc:3 }, { id: doc:1 }, { id: doc:3 }]"

[[test.results]]
error = "There was no suitable index supporting the expression: content @@ 'hello'"

[[test.results]]
error = "There was no suitable index supporting the expression: content @@ 'hello'"

*/
DEFINE ANALYZER simple TOKENIZERS blank,class;
DEFINE INDEX ft_doc ON doc FIELDS content FULLTEXT ANALYZER simple BM25;
INSERT INTO doc [
	{ id: doc:1, content: 'hello world' },
	{ id: doc:2, content: 'goodbye' },
	{ id: doc:3, content: 'hello again' },
];
INSERT INTO other [
	{ id: other:1, content: 'hello there' },
	{ id: other:2, content: 'nothing' },
];
-- `doc:2` does not match, though its table has the index.
SELECT id FROM doc, doc:2 WHERE content @@ 'hello';
SELECT id FROM doc, doc:1 WHERE content @@ 'hello';
SELECT id FROM doc, doc WHERE content @@ 'hello';
-- `other` has no full-text index.
SELECT id FROM doc, other WHERE content @@ 'hello';
SELECT id FROM doc, other WHERE content @@ 'hello' AND id != doc:3;
