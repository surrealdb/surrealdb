/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "An EXPLAIN written inside a block, a LET value, a loop body or an expression answers the rendering of the plan of the statement it explains, as it does at the top level."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:1, name: 'Alice' }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [table: person, index: by_name, access: = 'Alice', direction: Forward]
"'''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: person, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    CountScan [ctx: Db] [source: person]
''''

[[test.results]]
value = '''['SelectProject [ctx: Db] [projections: name]\n    RecordIdScan [ctx: Db] [record_id: person:1]\n', { attributes: { projections: '*' }, children: [{ attributes: { direction: 'Forward', limit: '1', table: 'person' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }]'''

*/

DEFINE TABLE person SCHEMALESS;
DEFINE INDEX by_name ON person FIELDS name;
CREATE person:1 SET name = 'Alice';

{
	LET $plan = (EXPLAIN SELECT * FROM person WHERE name = 'Alice');
	RETURN $plan;
};

{
	CREATE person:2 SET name = 'Bob';
	EXPLAIN SELECT * FROM person;
};

FOR $i IN [1] {
	RETURN EXPLAIN SELECT count() FROM person GROUP ALL;
};

RETURN [(EXPLAIN SELECT name FROM person:1), (EXPLAIN FORMAT JSON SELECT * FROM person LIMIT 1)];
