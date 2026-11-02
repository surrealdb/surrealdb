/**
[env]
namespace = true
database = true
imports = ["pgsql/sql_types/_ranges.surql"]

[test]
reason = "@> between two ranges has no lowering and is refused"

[test.results]
request-error = "PostgreSQL SELECT lowering error: unsupported expression: the @> containment operator"
*/

SELECT id FROM booking WHERE seats @> '[2,3)'::int4range;
