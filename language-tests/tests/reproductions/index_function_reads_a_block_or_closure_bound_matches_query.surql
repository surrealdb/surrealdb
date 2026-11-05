/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A search:: index function over a MATCHES whose query a block's LET or a
	closure's argument binds reads that query when the call runs, as it does
	for a FOR variable. The operator tree refuses both, for want of the value
	where it compiles the statement before the block or closure binds it;
	the value is bound by then wherever the call runs.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ body: 'hello world', id: doc:1 }]"

[[test.results]]
value = "[{ body: 'goodbye world', id: doc:2 }]"

[[test.results]]
value = "['<hello> world']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['<hello> world']"

[[test.results]]
value = "['hello <world>', 'goodbye <world>']"
*/
DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX ft ON doc FIELDS body FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
CREATE doc:1 SET body = 'hello world';
CREATE doc:2 SET body = 'goodbye world';
{ LET $q = 'hello'; RETURN SELECT VALUE search::highlight('<', '>', 1) FROM doc WHERE body @1@ $q; };
LET $f = |$q| (SELECT VALUE search::highlight('<', '>', 1) FROM doc WHERE body @1@ $q);
RETURN $f('hello');
RETURN $f('world');
