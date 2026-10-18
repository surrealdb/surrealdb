/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "An uncorrelated subquery on the right of IN is evaluated once using its own index while the outer table falls back to a filtered table scan and returns the matching rows"
issue = 5439

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: user:john, name: 'John' }]"

[[test.results]]
value = "[{ id: user:linda, name: 'Linda' }]"

[[test.results]]
value = "[{ id: house:big, owner: user:john }]"

[[test.results]]
value = "[{ id: house:small, owner: user:linda }]"

[[test.results]]
value = "[house:big]"

[[test.results]]
value = """{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'owner INSIDE (subquery)' }, children: [{ attributes: { direction: 'Forward', table: 'house' }, context: 'Db', operator: 'TableScan' }, { attributes: { strategy: 'once' }, children: [{ attributes: { expr: 'id' }, children: [{ attributes: { access: "= 'John'", direction: 'Forward', index: 'idx_name', table: 'user' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'ProjectValue' }], context: 'Db', operator: 'Subquery' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"""

[[test.results]]
value = '''{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: "owner INSIDE (\n  Project[id]\n    Filter[name = 'John']\n      Source[user]\n).id" }, children: [{ attributes: { direction: 'Forward', table: 'house' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'owner INSIDE [{ id: user:john }].id' }, children: [{ attributes: { direction: 'Forward', table: 'house' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"
*/

DEFINE TABLE user; DEFINE TABLE house;
DEFINE INDEX idx_name ON user COLUMNS name;
DEFINE INDEX idx_owner ON house COLUMNS owner;
CREATE user:john SET name = "John";
CREATE user:linda SET name = "Linda";
CREATE house:big SET owner = user:john;
CREATE house:small SET owner = user:linda;
SELECT VALUE id FROM house WHERE owner IN (SELECT VALUE id FROM user WHERE name = "John");
SELECT VALUE id FROM house WHERE owner IN (SELECT VALUE id FROM user WHERE name = "John") EXPLAIN;
SELECT VALUE id FROM house WHERE owner IN (SELECT id FROM user WHERE name = "John").id EXPLAIN;
LET $john = (SELECT id FROM user WHERE name = "John");
SELECT VALUE id FROM house WHERE owner IN $john.id EXPLAIN;
