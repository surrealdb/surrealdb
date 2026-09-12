/**
[test]
reason = """MATCHES behaves the same whether the table was written in the FROM or is only \
named when the statement runs. A `type::table(..)` source gets a full-text executor for \
the table it resolves to: with the index bypassed the operator is still judged per row, \
and a table without a suitable index raises the no-index error rather than matching \
nothing. A projection that repeats the WHERE's MATCHES with a parameter as its query is \
registered like the WHERE's, which received the parameter's value at bind time."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, name: 'hello world' }]"

[[test.results]]
value = "[{ id: u:1, name: 'hello there' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, m: true }]"

[[test.results]]
value = "[{ id: t:1, name: 'hello world' }]"

[[test.results]]
value = "[{ id: t:1, name: 'hello world' }]"

[[test.results]]
error = "There was no suitable index supporting the expression: name @@ 'hello'"
*/

DEFINE ANALYZER a TOKENIZERS blank FILTERS lowercase;
DEFINE INDEX ft ON t FIELDS name FULLTEXT ANALYZER a BM25;
CREATE t:1 SET name = 'hello world';
CREATE u:1 SET name = 'hello there';
LET $s = 'hello';

-- The projection repeats the WHERE's MATCHES with the parameter as its query.
SELECT id, name @@ $s AS m FROM t WHERE name @@ $s;

-- A table named when the statement runs, through the index and per row.
SELECT * FROM type::table('t') WHERE name @@ 'hello';
SELECT * FROM type::table('t') WITH NOINDEX WHERE name @@ 'hello';

-- A table without a suitable index raises the no-index error.
SELECT * FROM type::table('u') WHERE name @@ 'hello';
