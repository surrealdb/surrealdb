/**
[env]
namespace = true
database = true

[test]
reason = "ALTER DOMAIN ADD CONSTRAINT judges a stored NULL, object or array of a json domain by the CHECK, and its refusal names the constraint that failed and renders the value found as SurrealQL, as a write's refusal renders it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """[{ a: '[1, 2]', id: 1, n: NULL, o: '{"a": 1}' }]"""

[[test.results]]
error = "Invalid statement: Cannot change the type 'jn': the field 'n' on the table 't' holds a value it does not admit: Expected `jn (constraint present failed)` but found `NULL`"

[[test.results]]
error = """Invalid statement: Cannot change the type 'jo': the field 'o' on the table 't' holds a value it does not admit: Expected `jo (constraint never failed)` but found `'{"a": 1}'`"""

[[test.results]]
error = "Invalid statement: Cannot change the type 'ja': the field 'a' on the table 't' holds a value it does not admit: Expected `ja (constraint never failed)` but found `'[1, 2]'`"

[[test.results]]
value = """[{ a: '[1, 2]', id: 1, n: NULL, o: '{"a": 1}' }]"""

*/

CREATE DOMAIN jn AS json;

CREATE DOMAIN jo AS json;

CREATE DOMAIN ja AS json;

CREATE TABLE t (id int PRIMARY KEY, n jn, o jo, a ja);

INSERT INTO t (id, n, o, a) VALUES (1, NULL, '{"a": 1}', '[1, 2]');

ALTER DOMAIN jn ADD CONSTRAINT present CHECK (VALUE IS NOT NULL);

ALTER DOMAIN jo ADD CONSTRAINT never CHECK (false);

ALTER DOMAIN ja ADD CONSTRAINT never CHECK (false);

SELECT id, n, o, a FROM t ORDER BY id;
