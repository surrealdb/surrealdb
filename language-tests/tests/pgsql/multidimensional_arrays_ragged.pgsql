/**
[env]
namespace = true
database = true

[test]
reason = "A text literal cast to a multi-dimensional array whose sub-arrays differ in length is malformed, as PostgreSQL refuses it"

[test.results]
request-error = "PostgreSQL SELECT lowering error: invalid input syntax for type: malformed array literal: \"{{1,2},{3}}\""
*/

SELECT '{{1,2},{3}}'::int[][] AS g;
