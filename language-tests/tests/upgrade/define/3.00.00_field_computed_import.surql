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
value = "[{ created_at: d'2025-01-01T00:00:00Z', full_name: 'Alice Smith', id: emp:1, name: { first: 'Alice', last: 'Smith' } }]"

[[test.results]]
value = "[{ created_at: d'2025-02-01T00:00:00Z', full_name: 'Bob Jones', id: emp:2, name: { first: 'Bob', last: 'Jones' } }]"

*/
DEFINE TABLE emp SCHEMAFULL;
DEFINE FIELD name ON emp TYPE { first: string, last: string };
DEFINE FIELD full_name ON emp TYPE string VALUE string::concat(name.first, ' ', name.last);
DEFINE FIELD created_at ON emp TYPE datetime READONLY DEFAULT d'2025-01-01T00:00:00Z';
CREATE emp:1 SET name = { first: 'Alice', last: 'Smith' };
CREATE emp:2 SET name = { first: 'Bob', last: 'Jones' }, created_at = d'2025-02-01T00:00:00Z';
