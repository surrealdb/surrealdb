/**
[env]

[test]
reason = """
	Plan companion to matches_dynamic_sources: a MATCHES predicate
	over a table written in the FROM selects the full-text index, and the
	projection that repeats it is carried by the projection node above the
	same scan. A `type::table('..')` source of a constant name is resolved
	when the statement is planned, so the plan names the table and a MATCHES
	over it selects the full-text index; WITH NOINDEX keeps a table scan
	under a filter, and the no-index error for a table without a suitable
	index is raised when the statement runs, not by EXPLAIN.
"""

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
value = '''
'SelectProject [ctx: Db] [projections: id, m]
    FullTextScan [ctx: Db] [table: t, index: ft, query: hello]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    FullTextScan [ctx: Db] [table: t, index: ft, query: hello]
''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: name @@ 'hello']
        TableScan [ctx: Db] [table: t, direction: Forward]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: name @@ 'hello']
        TableScan [ctx: Db] [table: u, direction: Forward]
"'''
*/

DEFINE ANALYZER a TOKENIZERS blank FILTERS lowercase;
DEFINE INDEX ft ON t FIELDS name FULLTEXT ANALYZER a BM25;
CREATE t:1 SET name = 'hello world';
CREATE u:1 SET name = 'hello there';
LET $s = 'hello';

-- The projection repeats the WHERE's MATCHES with the parameter as its query.
EXPLAIN SELECT id, name @@ $s AS m FROM t WHERE name @@ $s;

-- A table named when the statement runs, through the index and per row.
EXPLAIN SELECT * FROM type::table('t') WHERE name @@ 'hello';
EXPLAIN SELECT * FROM type::table('t') WITH NOINDEX WHERE name @@ 'hello';

-- A table without a suitable index raises the no-index error.
EXPLAIN SELECT * FROM type::table('u') WHERE name @@ 'hello';
