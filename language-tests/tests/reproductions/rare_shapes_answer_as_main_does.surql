/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A grouped projection over a locking subquery is refused with the aggregate's error, a KNN operator projected beside the search answers true for the rows the search yields, a function under a VERSION only the run knows is called through its stored definition, and type::field in an element filter reads the element"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ age: 20, id: person:1, name: 'a' }]"

[[test.results]]
value = "[{ age: 30, id: person:2, name: 'b' }]"

[[test.results]]
value = "[{ id: knows:1, in: person:1, out: person:2 }]"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
error = """
Invalid query: Found a `$parent` parameter refering to the document of a GROUP select statement
Select statements with a GROUP BY or GROUP ALL currently have no defined document to refer to"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: t:1, v: [1, 2] }]"

[[test.results]]
value = "[{ a: 2, id: t:2, v: [1, 3] }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid query: KNN operators must appear at the top level of the WHERE clause (joined with AND); nesting `<|k, …|>` inside OR or NOT is not supported."

[[test.results]]
value = "[t:1, t:2, t:1, t:2]"

[[test.results]]
value = "[{ hit: true, id: t:1 }, { hit: true, id: t:2 }]"

[[test.results]]
value = "[t:1, t:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "There was a problem with the key-value store: The underlying datastore does not support versioned queries"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: u:x }]"

[[test.results]]
value = "[{ tags: NONE }, { tags: NONE }]"

[[test.results]]
value = "[{ id: person:9, tags: [{ x: 1 }, { x: 2 }] }]"

[[test.results]]
value = "[[{ x: 1 }]]"

























*/
DEFINE TABLE person SCHEMALESS; DEFINE TABLE knows TYPE RELATION; DEFINE TABLE t SCHEMALESS;
CREATE person:1 SET name = 'a', age = 20; CREATE person:2 SET name = 'b', age = 30;
RELATE person:1->knows:1->person:2;
SELECT * FROM ['a' @@ 'b'] FOR UPDATE;
SELECT a, $parent AS p FROM (SELECT * FROM t:1 FOR UPDATE) GROUP BY a;
DEFINE INDEX hv ON t FIELDS v HNSW DIMENSION 2;
CREATE t:1 SET v = [1, 2], a = 1; CREATE t:2 SET v = [1, 3], a = 2;
DEFINE INDEX ia ON t FIELDS a;
SELECT VALUE id FROM t WHERE v <|2,40|> [1, 2] OR a = 1;
SELECT VALUE id FROM t, t WHERE v <|2,40|> [1, 2];
SELECT id, (v <|2,40|> [1, 2]) AS hit FROM t WHERE v <|2,40|> [1, 2];
{ DEFINE INDEX hv2 ON t FIELDS v HNSW DIMENSION 2; RETURN SELECT VALUE id FROM t WHERE v <|2,40|> [1, 2]; };
{ DEFINE FIELD f ON t VALUE 1; UPDATE t SET a = 1 RETURN NONE; };
DEFINE FUNCTION fn::g() { RETURN 1 };
SELECT VALUE fn::g() FROM t VERSION time::now();
DEFINE FUNCTION fn::mk() { RETURN 'x' };
DEFINE FIELD id ON u DEFAULT fn::mk();
CREATE u;
SELECT tags[WHERE type::field('x') = 1] FROM person;
CREATE person:9 SET tags = [{x: 1}, {x: 2}];
SELECT VALUE tags[WHERE type::field('x') = 1] FROM person:9;
