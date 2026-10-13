/**
[env]
namespace = true
database = true

[test]
reason = "value::expect asserts on document fields inside a projection or a WHERE clause, on fields reached through a variable, and on a record id, which it returns as the id rather than the fetched record. The assertion closure is evaluated against the row being processed, so it can reach the rest of the document through $this. The assertion is not per-row: the first row that fails aborts the whole statement, so the client sees an error instead of a partially filtered result set. A bare `value::` path call cannot open a projection list, where the word `value` is reserved for the SELECT VALUE clause; parenthesising the call or placing it after another projection both parse"

[[test.results]]
value = "[{ age: 30, id: person:one }]"

[[test.results]]
value = "[{ age: 10, id: person:two }]"

[[test.results]]
value = "[{ age: 30, id: person:one }, { age: 10, id: person:two }]"

[[test.results]]
value = "[{ age: 30, id: person:one }, { age: 10, id: person:two }]"

[[test.results]]
value = "[{ age: 30, id: person:one, one: 1 }, { age: 10, id: person:two, one: 1 }]"

[[test.results]]
value = "[{ age: 30, id: person:one, one: 1 }, { age: 10, id: person:two, one: 1 }]"

[[test.results]]
value = "[{ id: person:one }]"

[[test.results]]
error = "An error occurred: value::expect assertion failed"

[[test.results]]
error = "An error occurred: value::expect assertion failed with message: 'too young'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "30"

[[test.results]]
value = "person:one"
*/

CREATE person:one SET age = 30;
CREATE person:two SET age = 10;

-- Method syntax on a document field, with every row satisfying the assertion
SELECT id, age.expect(|$a| $a > 5) AS age FROM person ORDER BY id;

-- Parenthesising lets a value:: path call open the projection list
SELECT (value::expect(age, |$a| $a > 5)) AS age, id FROM person ORDER BY id;

-- So does any non-leading projection position
SELECT 1 AS one, value::expect(age, |$a| $a > 5) AS age, id FROM person ORDER BY id;

-- The closure body evaluates against the row being projected
SELECT 1 AS one, value::expect(age, |$a| $this.age = $a) AS age, id FROM person ORDER BY id;

-- The assertion is also legal inside a WHERE clause
SELECT id FROM person WHERE age.expect(|$a| $a > 5) > 20 ORDER BY id;
SELECT id FROM person WHERE age.expect(|$a| $a > 20) > 5 ORDER BY id;

-- One failing row aborts the statement; no rows are returned
SELECT id, age.expect(|$a| $a > 20, "too young") AS age FROM person ORDER BY id;

-- A field reached through a variable
LET $p = (SELECT * FROM ONLY person:one);
RETURN $p.age.expect(|$a| $a = 30);

-- A record id asserts as itself and passes through unfetched
RETURN value::expect(person:one, |$r| $r.id = person:one);
