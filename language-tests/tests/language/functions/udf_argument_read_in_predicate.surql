/**
[env]
namespace = true
database = true

[test]
reason = """
A stored function whose body reads an argument where only a value can be
answered (a `WHERE` an index serves, a `LIMIT`, a source) is planned for each
call with that call's argument in force. Each call sees its own argument,
including calls made per row of an outer statement and calls nested in an
outer predicate, and an argument that does not coerce to its declared kind
fails the call before the body runs.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ age: 1, id: person:1, name: 'a' }]"

[[test.results]]
value = "[{ age: 2, id: person:2, name: 'b' }]"

[[test.results]]
value = "[{ age: 3, id: person:3, name: 'a' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[person:1, person:3]"

[[test.results]]
value = "[person:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: person:1, same: [person:1, person:3] }, { id: person:2, same: [person:2] }, { id: person:3, same: [person:1, person:3] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[person:1, person:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['a']"

[[test.results]]
value = "[person:1, person:3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[person:1, person:2, person:3], [person:2, person:3], []]"

[[test.results]]
error = "Incorrect arguments for function fn::older(). Failed to coerce argument `$min`: Expected `int` but found `'x'`"
*/
DEFINE TABLE person SCHEMALESS;
DEFINE INDEX name_idx ON person FIELDS name;
CREATE person:1 SET name = 'a', age = 1;
CREATE person:2 SET name = 'b', age = 2;
CREATE person:3 SET name = 'a', age = 3;
DEFINE FUNCTION fn::named($n: string) { RETURN SELECT VALUE id FROM person WHERE name = $n ORDER BY id; };
RETURN fn::named('a');
RETURN fn::named('b');
RETURN fn::named('zz');
SELECT id, fn::named(name) AS same FROM person ORDER BY id;
DEFINE FUNCTION fn::first($n: int) { RETURN SELECT VALUE id FROM person ORDER BY id LIMIT $n; };
RETURN fn::first(2);
RETURN fn::first(0);
DEFINE FUNCTION fn::get($id: record) { RETURN SELECT VALUE name FROM $id; };
RETURN fn::get(person:1);
SELECT VALUE id FROM person WHERE id IN fn::named('a') ORDER BY id;
DEFINE FUNCTION fn::older($min: int) { RETURN SELECT VALUE id FROM person WHERE age > $min ORDER BY id; };
RETURN [fn::older(0), fn::older(1), fn::older(5)];
RETURN fn::older('x');
