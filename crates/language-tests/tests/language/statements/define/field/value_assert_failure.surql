/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `age` of `person:test`: Expected `number` but found `NONE`"

[[test.results]]
error = "Couldn't coerce value for field `age` of `person:test`: Expected `number` but found `NONE`"

[[test.results]]
error = "Couldn't coerce value for field `age` of `person:test`: Expected `number` but found `NULL`"

[[test.results]]
error = "Found 0 for field `age`, with record `person:test`, but field must conform to: $value > 0"

[[test.results]]
value = "[{ age: 13, email: 'info@surrealdb.com', id: person:test, name: 'No name' }]"

*/

DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD age ON person TYPE number ASSERT $value > 0;
DEFINE FIELD email ON person TYPE string ASSERT string::is::email($value);
DEFINE FIELD name ON person TYPE option<string> VALUE $value OR 'No name';
CREATE person:test SET email = 'info@surrealdb.com', other = 'ignore';
CREATE person:test SET email = 'info@surrealdb.com', other = 'ignore', age = NONE;
CREATE person:test SET email = 'info@surrealdb.com', other = 'ignore', age = NULL;
CREATE person:test SET email = 'info@surrealdb.com', other = 'ignore', age = 0;
CREATE person:test SET email = 'info@surrealdb.com', other = 'ignore', age = 13;
