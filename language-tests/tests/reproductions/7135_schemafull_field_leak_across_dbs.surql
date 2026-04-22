/**
[env]
clean = true
namespace = true
database = false
auth = { level = "owner" }

[test]
reason = "SCHEMAFULL field definitions must not leak across databases in the same namespace"
issue = 7135

[[test.results]]
value = "NONE"

[[test.results]]
error = false

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
skip-record-id-key = true
value = "[{ email: 'alice@example.com', id: person:test, name: 'Alice' }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = false

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
skip-record-id-key = true
value = "[{ id: person:test, name: 'Bob' }]"
*/

-- Set up db_a with a SCHEMAFULL table that has name + email
DEFINE DATABASE db_a;
USE DB db_a;
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD email ON person TYPE string;
CREATE person SET name = 'Alice', email = 'alice@example.com';

-- Set up db_b with the same table name but only the name field
DEFINE DATABASE db_b;
USE DB db_b;
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;

-- This must succeed: db_b has no 'email' field, so creating without it is valid
CREATE person SET name = 'Bob';
