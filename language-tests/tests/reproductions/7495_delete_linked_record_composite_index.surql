/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "DELETE must not merge linked-record and local composite index predicates"
issue = 7495

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx_edge_in_type', joins: [{ index: 'idx_node_type', operator: '=', value: 'mm-Mission' }], operator: 'join' }, table: 'edge' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ edge_type: 'HAS', id: edge:has, in: node:mission, out: node:craft }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx_person_name_age', operator: '=', value: 'b' }, table: 'person' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ age: 20, friend: person:a, id: person:b, name: 'b' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ age: 20, friend: person:a, id: person:c, name: 'c' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ age: 20, friend: person:a, id: person:d, name: 'd' }]"

[[test.results]]
value = "[]"
*/

{
	DEFINE TABLE node SCHEMALESS;
	DEFINE TABLE edge SCHEMALESS;
	DEFINE FIELD in ON edge TYPE record<node>;
	DEFINE FIELD edge_type ON edge TYPE string;
	DEFINE INDEX idx_edge_in_type ON edge FIELDS in, edge_type;
	DEFINE INDEX idx_node_type ON node FIELDS node_type;
	CREATE node:mission SET node_type = 'mm-Mission';
	CREATE node:craft;
	RELATE node:mission -> edge:has -> node:craft CONTENT { edge_type: 'HAS' };
	RETURN 'OK';
};

DELETE edge
WHERE edge_type = 'HAS' AND in.node_type = 'mm-Mission'
EXPLAIN;

DELETE edge
WHERE edge_type = 'HAS' AND in.node_type = 'mm-Mission'
RETURN BEFORE;

SELECT * FROM edge;

{
	DEFINE TABLE person SCHEMALESS;
	DEFINE FIELD friend ON person TYPE record<person>;
	DEFINE INDEX idx_person_name_age ON person FIELDS name, age;
	DEFINE INDEX idx_person_friend ON person FIELDS friend;
	CREATE person:a SET name = 'a', age = 10, friend = person:a;
	CREATE person:b SET name = 'b', age = 20, friend = person:a;
	CREATE person:c SET name = 'c', age = 20, friend = person:a;
	CREATE person:d SET name = 'd', age = 20, friend = person:a;
	RETURN 'OK';
};

DELETE person
WHERE name = 'b' AND friend.age = 10
EXPLAIN;

DELETE person
WHERE name = 'b' AND friend.age = 10
RETURN BEFORE;

SELECT * FROM person:b;

DELETE person
WHERE name = 'c' AND friend.age IN [10]
RETURN BEFORE;

SELECT * FROM person:c;

DELETE person
WHERE name = 'd' AND friend.age < 15
RETURN BEFORE;

SELECT * FROM person:d;
