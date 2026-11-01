/**
[env]
namespace = true
database = true
imports = ["pgsql/_drop_schema_tables.surql"]

[test]
reason = "DROP SCHEMA public CASCADE removes a view created earlier in the same request before the table it reads"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ n: 0 }]"
*/

CREATE VIEW dsc_zview AS SELECT * FROM dsc_author;

DROP SCHEMA public CASCADE;

CREATE TABLE dsc_author (id INT PRIMARY KEY);

SELECT count(*) AS n FROM dsc_author;
