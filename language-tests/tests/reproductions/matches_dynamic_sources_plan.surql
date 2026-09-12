/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = """
	Plan companion to matches_dynamic_sources: a MATCHES predicate
	over a table written in the FROM selects the full-text index, and the
	projection that repeats it is carried above the same scan. A
	`type::table(..)` source is resolved when the statement runs, so the plan
	shows a scan over the expression rather than a named table; WITH NOINDEX
	keeps that shape without an index, and the no-index error for a table
	without a suitable index is raised when the statement runs, not by EXPLAIN.
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
"SelectProject [ctx: Db] [projections: id, m]
    Compute [ctx: Db] [fields: m = name @@ 'hello']
        FullTextScan [ctx: Db] [index: ft, query: hello]
"'''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    FullTextScan [ctx: Db] [index: ft, query: hello]
''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: t, direction: Forward, predicate: name @@ 'hello', pre_decode_filter: no (unsupported predicate)]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: u, direction: Forward, predicate: name @@ 'hello', pre_decode_filter: no (unsupported predicate)]
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
