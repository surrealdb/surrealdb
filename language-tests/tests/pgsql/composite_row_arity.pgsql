/**
[env]
namespace = true
database = true

[test]
reason = "A row cast to a composite type of another number of attributes is refused when the request is planned"

[test.results]
request-error = "PostgreSQL SELECT lowering error: data exception: cannot cast a row of 1 values to the type address of 2 attributes"
*/

CREATE TYPE address AS (street text, zip int);

SELECT ROW('Short')::address AS a;
