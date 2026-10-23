/**
[env]
namespace = true
database = true
imports = ["pgsql/_union_tables.surql"]

[test]
reason = "A chain of more than two queries that mixes UNION and UNION ALL reads differently under different groupings, so the request is refused rather than read one way"

[test.results]
request-error = "PostgreSQL SELECT lowering error: unsupported expression: a chain of more than two queries must join them all with UNION or all with UNION ALL; group a mix with subqueries"
*/

SELECT name FROM un_a UNION ALL SELECT label FROM un_b UNION SELECT name FROM un_a;
