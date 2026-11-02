/**
[env]
namespace = true
database = true

[test]
reason = "a point cast from text that does not read as a point is refused when the statement is lowered"

[test.results]
request-error = 'PostgreSQL SELECT lowering error: invalid input syntax for type: invalid input syntax for type point: "(1," (not a number: "(1")'
*/

SELECT '(1,'::point;
