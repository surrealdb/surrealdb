/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL CURRENT_DATE is the start of a day in a write as in a read"

[[test.results]]
value = "NONE"

[[test.results]]
match = "array::len($result) == 1 && array::all($result, |$row| $row.at == time::floor($row.at, 1d))"

[[test.results]]
value = "[{ returned: true }]"

[[test.results]]
value = "[{ id: 1, today: true }]"
*/

CREATE TABLE days (id INT, at TIMESTAMP);

INSERT INTO days (id, at) VALUES (1, CURRENT_DATE);

UPDATE days SET at = CURRENT_DATE WHERE id = 1 RETURNING at = current_date AS returned;

SELECT id, at = current_date AS today FROM days;
