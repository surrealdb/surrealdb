/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A search:: index function over a MATCHES whose query a FOR binds reads
	the query the loop holds when the call runs. One whose MATCHES query is
	no literal or parameter registers no clause, and is refused as the
	statement is planned.
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
value = "NONE"

[[test.results]]
value = "[{ q: 'goodbye', r: ['<b>goodbye</b> world'] }, { q: 'hello', r: ['<b>hello</b> world'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ q: 'world', s: [true, true] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '[{ id: doc:1, o: { "0": [{ e: 11, s: 6 }] }, s: true }, { id: doc:2, o: { "0": [{ e: 13, s: 8 }] }, s: true }]'

[[test.results]]
value = "[{ h: '<b>hello</b> world', id: doc:1 }]"

[[test.results]]
error = "Invalid query: Index function 'search::highlight': no MATCHES clause found in WHERE condition"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['hello <world>', 'goodbye <world>']"
*/
DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX ft ON doc FIELDS body FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
CREATE doc:1 SET body = 'hello world';
CREATE doc:2 SET body = 'goodbye world';
FOR $q IN ['hello', 'goodbye'] { CREATE res CONTENT { q: $q, r: (SELECT VALUE search::highlight('<b>', '</b>', 1) FROM doc WHERE body @1@ $q) }; };
SELECT q, r FROM res ORDER BY q;
FOR $q IN ['world'] { CREATE scored CONTENT { q: $q, s: (SELECT VALUE search::score(1) >= 0 FROM doc WHERE body @1@ $q ORDER BY id) }; };
SELECT q, s FROM scored;
LET $w = 'world';
SELECT id, search::score(1) > 0 AS s, search::offsets(1) AS o FROM doc WHERE body @1@ $w ORDER BY id;
SELECT id, search::highlight('<b>', '</b>', 1) AS h FROM doc WHERE body @1@ string::concat('hel', 'lo');
RETURN (SELECT VALUE search::highlight('[', ']', 1) FROM doc WHERE body @1@ (SELECT VALUE 'goodbye' FROM ONLY doc:1));
DEFINE FUNCTION fn::hl($q: string) { RETURN SELECT VALUE search::highlight('<', '>', 1) FROM doc WHERE body @1@ $q };
RETURN fn::hl('world');
