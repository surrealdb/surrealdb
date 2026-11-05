/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL quote_ident applied to column values at run time, including NULL and keywords"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1, n: 'plain' }, { id: 2, n: 'CompatUser' }, { id: 3, n: 'select' }, { id: 4, n: 'has\"quote' }, { id: 5, n: NULL }]"

[[test.results]]
value = "[{ id: 1, q: 'plain' }, { id: 2, q: '\"CompatUser\"' }, { id: 3, q: '\"select\"' }, { id: 4, q: '\"has\"\"quote\"' }, { id: 5, q: NULL }]"
*/

CREATE TABLE idents (id INT, n VARCHAR(50));
INSERT INTO idents (id, n) VALUES (1, 'plain'), (2, 'CompatUser'), (3, 'select'), (4, 'has"quote'), (5, NULL);
SELECT id, quote_ident(n) AS q FROM idents ORDER BY id;
