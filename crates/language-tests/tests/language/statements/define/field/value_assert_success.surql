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
value = "[{ age: 22, email: 'info@surrealdb.com', id: person:test, name: 'No name' }]"

*/

DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD age ON person TYPE number ASSERT $value > 0;
DEFINE FIELD email ON person TYPE string ASSERT string::is::email($value);
DEFINE FIELD name ON person TYPE option<string> VALUE $value OR 'No name';
CREATE person:test SET email = 'info@surrealdb.com', other = 'ignore', age = 22;
