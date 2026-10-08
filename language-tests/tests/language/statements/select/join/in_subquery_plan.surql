/**
[env]
namespace = true
database = true

[test]
reason = """
	Plan companion to in_subquery: an `IN (SELECT VALUE id FROM t …)` keyed
	on the target's id reads the target by record id, one batched read per
	left batch, with the subquery's filter applied to what the read yields;
	an `IN` keyed on any other field, or on several, builds the right side
	into a hash table. The left side is the pipeline's source, and the other
	conjuncts of the predicate filter it beneath the join. A shape the pass
	does not lift — a `LIMIT`, a `MATCHES` in the subquery's filter — stays a
	scalar subquery inside the filter.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: team INSIDE (subquery)]
        TableScan [ctx: Db] [table: person, direction: Forward]
        Subquery [ctx: Db] [strategy: once]
            ProjectValue [ctx: Db] [expr: id]
                TableScan [ctx: Db] [table: team, direction: Forward, predicate: active = true, pre_decode_filter: yes]
''''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: name != 'p1' AND team NOT INSIDE (subquery)]
        TableScan [ctx: Db] [table: person, direction: Forward]
        Subquery [ctx: Db] [strategy: once]
            ProjectValue [ctx: Db] [expr: id]
                TableScan [ctx: Db] [table: team, direction: Forward]
"'''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: team INSIDE (subquery)]
        TableScan [ctx: Db] [table: person, direction: Forward]
        Subquery [ctx: Db] [strategy: apply]
            ProjectValue [ctx: Db] [expr: team]
                Filter [ctx: Db] [predicate: who = $parent.id]
                    TableScan [ctx: Db] [table: grant, direction: Forward, predicate: n > 1, pre_decode_filter: yes]
''''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: 'A' INSIDE (subquery)]
        TableScan [ctx: Db] [table: person, direction: Forward]
        Subquery [ctx: Db] [strategy: once]
            ProjectValue [ctx: Db] [expr: name]
                TableScan [ctx: Db] [table: team, direction: Forward]
"'''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: team INSIDE (subquery)]
        TableScan [ctx: Db] [table: person, direction: Forward]
        Subquery [ctx: Db] [strategy: once]
            ProjectValue [ctx: Db] [expr: id]
                TableScan [ctx: Db] [table: team, direction: Forward, predicate: active = true, limit: 1, pre_decode_filter: yes]
''''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: team INSIDE (subquery)]
        TableScan [ctx: Db] [table: person, direction: Forward]
        Subquery [ctx: Db] [strategy: apply]
            ProjectValue [ctx: Db] [expr: id]
                FullTextScan [ctx: Db] [table: team, index: team_name, query: A]
''''
*/

DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD team ON person TYPE option<record<team>>;
DEFINE TABLE team SCHEMAFULL;
DEFINE FIELD name ON team TYPE string;
DEFINE FIELD active ON team TYPE bool;
DEFINE TABLE grant SCHEMALESS;
EXPLAIN SELECT VALUE id FROM person WHERE team IN (SELECT VALUE id FROM team WHERE active = true);
EXPLAIN SELECT VALUE id FROM person WHERE name != 'p1' AND team NOT IN (SELECT VALUE id FROM team);
EXPLAIN SELECT VALUE id FROM person WHERE (SELECT VALUE team FROM grant WHERE who = $parent.id AND n > 1) CONTAINS team;
EXPLAIN SELECT VALUE id FROM person WHERE 'A' IN (SELECT VALUE name FROM team);
EXPLAIN SELECT VALUE id FROM person WHERE team IN (SELECT VALUE id FROM team WHERE active = true LIMIT 1);
DEFINE ANALYZER simple TOKENIZERS blank;
DEFINE INDEX team_name ON team FIELDS name FULLTEXT ANALYZER simple BM25;
EXPLAIN SELECT VALUE id FROM person WHERE team IN (SELECT VALUE id FROM team WHERE name @@ 'A');
