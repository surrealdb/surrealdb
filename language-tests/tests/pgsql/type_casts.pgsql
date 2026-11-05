/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL explicit CAST expressions"

[[test.results]]
value = "[{ float_val: 3.14f, int_val: 42, str_val: 'hello' }]"
*/

SELECT CAST('42' AS INTEGER) AS int_val, CAST('3.14' AS FLOAT) AS float_val, CAST('hello' AS VARCHAR) AS str_val;
