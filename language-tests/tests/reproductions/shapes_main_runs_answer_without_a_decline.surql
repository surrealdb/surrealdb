/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	Shapes the operator tree runs, or refuses with an error of its own, answer as
	it answers them rather than meeting a decline: `*` beside fields over the rows
	of a statement in FROM, an index operator over a value source, FETCH into a
	block or INFO value, EXPLAIN ANALYZE of an EXPLAIN in a block, an EXPLAIN of a
	protected LET, a recursion body that writes, a generated key in a lookup range,
	a view named by a parameter or a call, a relation `in` field given a non-record
	kind, a function alias, access-method clauses the body refuses, and a locked
	read of a lightweight edge.
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
value = "[{ age: 20, id: person:1, name: 'a', tags: ['x'] }]"

[[test.results]]
value = "[{ age: 30, id: person:2, name: 'b' }]"

[[test.results]]
value = "[{ a: 1, id: post:1 }]"

[[test.results]]
value = "[{ id: knows:1, in: person:1, out: person:2 }]"

[[test.results]]
value = "[{ age: 20, id: person:1, name: 'a', one: 1, tags: ['x'] }, { age: 30, id: person:2, name: 'b', one: 1 }]"

[[test.results]]
value = "[{ age: 20, id: person:1, name: 'a', one: 1, tags: ['x'] }, { age: 30, id: person:2, name: 'b', one: 1 }, { a: 1, id: post:1, one: 1 }]"

[[test.results]]
value = "[false]"

[[test.results]]
value = "{ x: 1 }"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, extensions: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: { knows: 'DEFINE TABLE knows TYPE RELATION SCHEMALESS PERMISSIONS NONE', person: 'DEFINE TABLE person TYPE ANY SCHEMALESS PERMISSIONS NONE', post: 'DEFINE TABLE post TYPE ANY SCHEMALESS PERMISSIONS NONE', t: 'DEFINE TABLE t TYPE ANY SCHEMALESS PERMISSIONS NONE' }, users: {  } }"

[[test.results]]
value = '''
"ExprPlan [ctx: Rt] [expr: 'SelectProject [ctx: Db] [projections: *]\\n    TableScan [ctx: Db] [table: person, direction: Forward]\\n'] {rows: 1}

Total rows: 1"'''

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
error = "Internal database error: reading the definition of table `person`: The underlying datastore does not support versioned queries"

[[test.results]]
value = "[person:2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:5, next: person:6 }]"

[[test.results]]
value = "[{ id: person:6 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid query: Generated keys (rand, ulid, uuid) cannot be used in graph range bounds"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Expected `table name` but found `v2`"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The field 'in' already exists"

[[test.results]]
error = "Invalid statement: GRAPHQL_ALIAS `1bad` on function is not a valid GraphQL Name; expected /[_A-Za-z][_0-9A-Za-z]\u002A/"

[[test.results]]
error = "Could not cast into `duration` using input `'abc'`"

[[test.results]]
error = "Expected `string` but found `123`"

[[test.results]]
error = "Invalid query: Symmetric algorithm HS512 requires the same key for signing and verification. Use the same key value for both KEY and WITH ISSUER KEY clauses, or omit WITH ISSUER KEY."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: [{ age: 20, b: [1, 1, 1, 1], id: person:1, name: 'a', tags: ['x'] }], age: 30, id: person:2, name: 'b' }]"




































































*/
DEFINE TABLE person SCHEMALESS; DEFINE TABLE post SCHEMALESS; DEFINE TABLE knows TYPE RELATION; DEFINE TABLE t SCHEMALESS;
CREATE person:1 SET name = 'a', age = 20, tags = ['x']; CREATE person:2 SET name = 'b', age = 30; CREATE post:1 SET a = 1;
RELATE person:1->knows:1->person:2;
SELECT *, 1 AS one FROM (SELECT * FROM person);
SELECT *, 1 AS one FROM person, post;
SELECT * FROM ['a' @@ 'b'];
RETURN { LET $a = { x: 1 }; $a } FETCH x;
RETURN INFO FOR DB FETCH x;
{ EXPLAIN ANALYZE EXPLAIN SELECT * FROM person };
EXPLAIN LET $auth = 1;
RETURN (SELECT * FROM person:1 VERSION d'2024-01-01T00:00:00Z' FOR UPDATE);
{ LET $m = 18; RETURN person:1.{1..3}(->knows->person[WHERE age > $m]); };
DEFINE FUNCTION fn::w($v: any) { CREATE t; RETURN true; };
CREATE person:5 SET next = person:6; CREATE person:6;
RETURN person:5.{1..2}(.next[WHERE fn::w(id)]);
SELECT ->knows:rand()..->person FROM person:1;
LET $tb = "v"; DEFINE TABLE $tb AS SELECT * FROM person;
DEFINE TABLE type::table("v2") AS SELECT * FROM person;
DEFINE TABLE likes TYPE RELATION; DEFINE FIELD in ON likes TYPE string;
DEFINE FUNCTION fn::g() { RETURN 1 } GRAPHQL_ALIAS "1bad";
DEFINE ACCESS a1 ON DATABASE TYPE RECORD DURATION FOR SESSION "abc";
DEFINE ACCESS a2 ON DATABASE TYPE JWT ALGORITHM HS512 KEY 123;
DEFINE ACCESS a3 ON DATABASE TYPE JWT ALGORITHM HS512 KEY "a" WITH ISSUER KEY "b";
DEFINE TABLE follows TYPE RELATION IN person OUT person LIGHTWEIGHT; SELECT * FROM follows:x FOR UPDATE;
UPDATE person SET a = (UPDATE person:1 SET b = (SELECT VALUE 1 FROM person)) WHERE id = person:2;
