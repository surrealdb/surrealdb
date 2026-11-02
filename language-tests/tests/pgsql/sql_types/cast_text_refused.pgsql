/**
[env]
namespace = true
database = true

[test]
reason = "a cast from text that does not read as the target type is refused when the statement is lowered"

[test.results]
request-error = 'PostgreSQL SELECT lowering error: invalid input syntax for type: invalid input syntax for type macaddr: "not a mac"'
*/

SELECT 'not a mac'::macaddr;
