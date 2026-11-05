/**
[env]
namespace = true
database = true

[test]
reason = """
	The predicate of a statement whose `IN` subquery became a join is split
	around the join: the membership conjunct is the join, and every other
	conjunct reads the left row — by a field's name or through `$this`, which
	a semi join leaves as it is — so they filter the left input beneath the
	join, where an index over one of them drives the scan and the scan applies
	the rest per row; a conjunct an index does not answer beside one it does
	is judged over the index's rows before the join. The subquery's own
	filter is the right side's.
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
value = "NONE"

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: team INSIDE (subquery)]
        IndexScan [ctx: Db] [table: person, index: person_name, access: = 'p1', direction: Forward]
        Subquery [ctx: Db] [strategy: once]
            ProjectValue [ctx: Db] [expr: id]
                TableScan [ctx: Db] [table: team, direction: Forward, predicate: active = true, pre_decode_filter: yes]
"'''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: age > 3 AND team INSIDE (subquery)]
        TableScan [ctx: Db] [table: person, direction: Forward]
        Subquery [ctx: Db] [strategy: once]
            ProjectValue [ctx: Db] [expr: id]
                TableScan [ctx: Db] [table: team, direction: Forward, predicate: active = true, pre_decode_filter: yes]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: age > 3 AND string::len($this.name) > 1 AND team INSIDE (subquery)]
        TableScan [ctx: Db] [table: person, direction: Forward]
        Subquery [ctx: Db] [strategy: once]
            ProjectValue [ctx: Db] [expr: id]
                TableScan [ctx: Db] [table: team, direction: Forward, predicate: active = true, pre_decode_filter: yes]
''''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: age > 3 AND team INSIDE (subquery)]
        IndexScan [ctx: Db] [table: person, index: person_name, access: = 'p1', direction: Forward]
        Subquery [ctx: Db] [strategy: once]
            ProjectValue [ctx: Db] [expr: id]
                TableScan [ctx: Db] [table: team, direction: Forward, predicate: active = true, pre_decode_filter: yes]
"'''
*/

DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD age ON person TYPE int;
DEFINE FIELD team ON person TYPE option<record<team>>;
DEFINE TABLE team SCHEMAFULL;
DEFINE FIELD name ON team TYPE string;
DEFINE FIELD active ON team TYPE bool;
DEFINE INDEX person_name ON person FIELDS name;
EXPLAIN SELECT VALUE id FROM person WHERE name = 'p1' AND team IN (SELECT VALUE id FROM team WHERE active = true);
EXPLAIN SELECT VALUE id FROM person WHERE age > 3 AND team IN (SELECT VALUE id FROM team WHERE active = true);
EXPLAIN SELECT VALUE id FROM person WHERE age > 3 AND string::len($this.name) > 1 AND team IN (SELECT VALUE id FROM team WHERE active = true);
EXPLAIN SELECT VALUE id FROM person WHERE age > 3 AND team IN (SELECT VALUE id FROM team WHERE active = true) AND name = 'p1';
