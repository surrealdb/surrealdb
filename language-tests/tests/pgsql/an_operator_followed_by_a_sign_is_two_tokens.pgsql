/**
[env]
namespace = true
database = true
imports = ["pgsql/_json_shapes.surql"]

[test]
reason = "An operator of several characters does not end in a sign, as PostgreSQL reads it: ->>-1 is ->> and -1, and idx>-1 is > and -1"

[[test.results]]
value = """[{ before_last: [3, [4, 5]], last: { x: 'in-array' }, text_last: '{"x":"in-array"}' }]"""

[[test.results]]
value = "[{ name: 'arr_mixed' }, { name: 'arr_nested' }]"

[[test.results]]
value = "[{ name: 'arr_mixed' }, { name: 'obj_flat' }]"

[[test.results]]
value = "[]"
*/

SELECT data->>-1 AS text_last, data->-1 AS last, data->-2 AS before_last FROM jdoc WHERE name = 'arr_nested';

SELECT name FROM jdoc WHERE data->>-1 IS NOT NULL ORDER BY name;

SELECT name FROM jdoc WHERE idx>-1 ORDER BY name;

SELECT name FROM jdoc WHERE idx=-1 ORDER BY name;
