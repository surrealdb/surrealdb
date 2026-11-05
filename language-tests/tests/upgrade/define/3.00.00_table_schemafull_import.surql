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
value = "NONE"

[[test.results]]
value = "[{ age: 30, email: 'alice@example.com', id: account:1, name: 'Alice', tags: ['admin', 'rust'] }]"

[[test.results]]
value = "[{ age: 25, email: 'bob@example.com', id: account:2, name: 'Bob', tags: [] }]"

*/
DEFINE TABLE account SCHEMAFULL;
DEFINE FIELD name ON account TYPE string ASSERT $value != NONE;
DEFINE FIELD age ON account TYPE int ASSERT $value >= 0 AND $value < 200;
DEFINE FIELD email ON account TYPE string ASSERT string::is_email($value);
DEFINE FIELD tags ON account TYPE array<string> DEFAULT [];
CREATE account:1 SET name = 'Alice', age = 30, email = 'alice@example.com', tags = ['admin', 'rust'];
CREATE account:2 SET name = 'Bob', age = 25, email = 'bob@example.com';
