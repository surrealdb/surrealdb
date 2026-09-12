/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

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
    LookupJoin [ctx: Db] [type: Semi, alias: _join0, table: team, on: team = _join0.id]
        TableScan [ctx: Db] [table: person, direction: Forward]
        Filter [ctx: Db] [predicate: active = true]
            RecordBatchFetch [ctx: Db] [table: team]
''''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    LookupJoin [ctx: Db] [type: Anti, alias: _join0, table: team, on: team = _join0.id]
        TableScan [ctx: Db] [table: person, direction: Forward, predicate: name != 'p1', pre_decode_filter: yes]
        RecordBatchFetch [ctx: Db] [table: team]
"'''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    HashJoin [ctx: Db] [type: Semi, alias: _join0, on: team = _join0.team AND id = _join0.who]
        TableScan [ctx: Db] [table: person, direction: Forward]
        TableScan [ctx: Db] [table: grant, direction: Forward, predicate: n > 1, pre_decode_filter: yes]
''''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    HashJoin [ctx: Db] [type: Semi, alias: _join0, on: 'A' = _join0.name]
        TableScan [ctx: Db] [table: person, direction: Forward]
        TableScan [ctx: Db] [table: team, direction: Forward]
"'''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: person, direction: Forward, predicate: team INSIDE (ProjectValue), pre_decode_filter: no (unsupported predicate)]
''''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: person, direction: Forward, predicate: team INSIDE (ProjectValue), pre_decode_filter: no (unsupported predicate)]
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
