/**
[env]
namespace = true
database = true
imports = ["pgsql/_json_shapes.surql"]

[env.capabilities]
deny-functions = ["array::at"]

[test]
reason = "PostgreSQL JSON operators that take an element from the start of an array or the last element, or a path step that names one, read the array without calling array::at or type::is_array, so a deployment that denies those functions still runs them, and a scalar gives NULL under a path"

[[test.results]]
value = "[{ name: 'absent', v: NULL }, { name: 'arr_mixed', v: 'two' }, { name: 'arr_nested', v: [3, [4, 5]] }, { name: 'obj_arr', v: NULL }, { name: 'obj_deep', v: NULL }, { name: 'obj_flat', v: NULL }, { name: 'obj_keys', v: NULL }, { name: 'obj_nest', v: NULL }, { name: 'scalar_bool', v: NULL }, { name: 'scalar_null', v: NULL }, { name: 'scalar_num', v: NULL }, { name: 'scalar_str', v: NULL }]"

[[test.results]]
value = "[{ name: 'absent', v: NULL }, { name: 'arr_mixed', v: 'two' }, { name: 'obj_keys', v: NULL }, { name: 'scalar_str', v: NULL }]"

[[test.results]]
value = "[{ p0: '1', p1: 'two', p2: NULL, p4: NULL }]"

[[test.results]]
value = "[{ m1: { x: 1 }, p3: { x: 1 }, p4: NULL }]"

[[test.results]]
value = "[{ name: 'absent', v: NULL }, { name: 'arr_mixed', v: { x: 1 } }, { name: 'arr_nested', v: { x: 'in-array' } }, { name: 'obj_arr', v: NULL }, { name: 'obj_deep', v: NULL }, { name: 'obj_flat', v: NULL }, { name: 'obj_keys', v: NULL }, { name: 'obj_nest', v: NULL }]"

[[test.results]]
value = "[{ name: 'absent', v: NULL }, { name: 'obj_flat', v: NULL }, { name: 'obj_keys', v: NULL }]"

[[test.results]]
value = "[{ last_of_last: '5' }]"

[[test.results]]
value = "[{ k0: 'zero', k01: 'oh-one', km1: 'minus', kp1: NULL }]"

[[test.results]]
value = """[{ s01: 'two', sm1: '{"x":1}', sm1x: '1', sp1: 'two' }]"""

[[test.results]]
value = "[{ f01: '20', fm1: '30' }]"

[[test.results]]
value = "[{ name: 'obj_deep' }]"

[[test.results]]
value = "[{ a1m1: [4, 5], p11m1: '5' }]"

[[test.results]]
value = "[{ last: 'bottom' }]"

[[test.results]]
value = "[{ m1: NULL, name: 'absent', nested: NULL, p1: NULL, z1: NULL, zero: NULL }, { m1: NULL, name: 'scalar_bool', nested: NULL, p1: NULL, z1: NULL, zero: NULL }, { m1: NULL, name: 'scalar_null', nested: NULL, p1: NULL, z1: NULL, zero: NULL }, { m1: NULL, name: 'scalar_num', nested: NULL, p1: NULL, z1: NULL, zero: NULL }, { m1: NULL, name: 'scalar_str', nested: NULL, p1: NULL, z1: NULL, zero: NULL }]"
*/

SELECT name, data -> 1 AS v FROM jdoc ORDER BY name;

SELECT name, data ->> 1 AS v FROM jdoc WHERE name IN ('arr_mixed', 'obj_keys', 'scalar_str', 'absent') ORDER BY name;

SELECT data ->> 0 AS p0, data ->> 1 AS p1, data ->> 2 AS p2, data ->> 4 AS p4 FROM jdoc WHERE name = 'arr_mixed';

SELECT data -> 3 AS p3, data -> -1 AS m1, data -> 4 AS p4 FROM jdoc WHERE name = 'arr_mixed';

SELECT name, data -> -1 AS v FROM jdoc WHERE name NOT LIKE 'scalar%' ORDER BY name;

SELECT name, data ->> -1 AS v FROM jdoc WHERE name IN ('obj_keys', 'obj_flat', 'absent') ORDER BY name;

SELECT data -> 1 -> -1 ->> -1 AS last_of_last FROM jdoc WHERE name = 'arr_nested';

SELECT data #>> '{0}' AS k0, data #>> '{-1}' AS km1, data #>> '{01}' AS k01, data #>> '{+1}' AS kp1 FROM jdoc WHERE name = 'obj_keys';

SELECT data #>> '{-1}' AS sm1, data #>> '{01}' AS s01, data #>> '{+1}' AS sp1, data #>> '{-1,x}' AS sm1x FROM jdoc WHERE name = 'arr_mixed';

SELECT data #>> '{e,f,-1}' AS fm1, data #>> '{e,f,01}' AS f01 FROM jdoc WHERE name = 'obj_deep';

SELECT name FROM jdoc WHERE data #>> '{e,f,-1}' = '30';

SELECT data #>> '{1,1,-1}' AS p11m1, data -> 1 -> -1 AS a1m1 FROM jdoc WHERE name = 'arr_nested';

SELECT data #>> '{l1,l2,l3,l4,l5,-1}' AS last FROM jdoc WHERE name = 'obj_nest';

SELECT name, data #>> '{-1}' AS m1, data #>> '{01}' AS z1, data #>> '{+1}' AS p1, data #>> '{0}' AS zero, data #>> '{0,-1}' AS nested FROM jdoc WHERE name LIKE 'scalar%' OR name = 'absent' ORDER BY name;
