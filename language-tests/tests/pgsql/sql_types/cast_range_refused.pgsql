/**
[env]
namespace = true
database = true

[test]
reason = "a range cast from text whose lower bound exceeds its upper bound is refused when the statement is lowered"

[test.results]
request-error = "PostgreSQL SELECT lowering error: data exception: range lower bound must be less than or equal to range upper bound"
*/

SELECT '[5,1]'::int4range;
