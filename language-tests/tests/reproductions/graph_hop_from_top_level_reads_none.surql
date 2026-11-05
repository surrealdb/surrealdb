/**
[env]
namespace = true
database = true

[test]
reason = "A graph lookup that starts from the current value, written where no current value is bound, evaluates over NONE: the top-level forms read no records and the statement forms count zero. A bound current value (a subquery in a projection, count(<-edge)) keeps reading the records it reaches."

[[test.results]]
value = "[{ id: person:a }, { id: person:b }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: likes:ab, in: person:a, out: person:b }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[[{ count: 0 }], [{ count: 1 }]]"

[[test.results]]
value = "[0, 1]"

[[test.results]]
value = "NONE"

*/

CREATE person:a, person:b;
DEFINE TABLE likes TYPE RELATION FROM person TO person;
RELATE person:a->likes:ab->person:b;
RETURN <-likes;
SELECT * FROM <-likes;
SELECT * FROM ->likes;
SELECT count() FROM <-likes GROUP ALL;
SELECT count() FROM <-likes<-person GROUP ALL;
SELECT VALUE (SELECT count() FROM <-likes GROUP ALL) FROM person ORDER BY id;
SELECT VALUE count(<-likes) FROM person ORDER BY id;
RETURN ->likes->person;
