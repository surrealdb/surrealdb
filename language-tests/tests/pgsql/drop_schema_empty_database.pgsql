/**
[env]
namespace = true
database = true

[test]
reason = "DROP SCHEMA public, with or without CASCADE, answers once in a database that holds no table"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ n: 0 }]"
*/

DROP SCHEMA public;

DROP SCHEMA public CASCADE;

CREATE SCHEMA public;

CREATE TABLE dse_item (id INT PRIMARY KEY);

SELECT count(*) AS n FROM dse_item;
