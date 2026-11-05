/**
[env]
namespace = true
database = true
imports = ["pgsql/_json_shapes.surql"]

[test]
reason = "The operators of a chain read each member from the one before it, so an absent member, a JSON null and a scalar anywhere along it reach nothing, and a path starts from a chain the same way; IS [NOT] NULL of a JSON operator holds exactly when the operator gives NULL"

[[test.results]]
value = "[{ name: 'absent', v: NULL }, { name: 'arr_mixed', v: NULL }, { name: 'arr_nested', v: NULL }, { name: 'obj_arr', v: NULL }, { name: 'obj_deep', v: NULL }, { name: 'obj_flat', v: NULL }, { name: 'obj_keys', v: NULL }, { name: 'obj_nest', v: NULL }, { name: 'scalar_bool', v: NULL }, { name: 'scalar_null', v: NULL }, { name: 'scalar_num', v: NULL }, { name: 'scalar_str', v: NULL }]"

[[test.results]]
value = "[{ absent: NULL, last: 2, nested: '1', null_member: NULL, scalar: NULL }]"

[[test.results]]
value = "[{ name: 'absent', v: NULL }, { name: 'arr_mixed', v: NULL }, { name: 'arr_nested', v: NULL }, { name: 'obj_arr', v: NULL }, { name: 'obj_deep', v: NULL }, { name: 'obj_flat', v: NULL }, { name: 'obj_keys', v: NULL }, { name: 'obj_nest', v: NULL }, { name: 'scalar_bool', v: NULL }, { name: 'scalar_null', v: NULL }, { name: 'scalar_num', v: NULL }, { name: 'scalar_str', v: NULL }]"

[[test.results]]
value = "[{ v: '1' }]"

[[test.results]]
value = "[{ m1: '30', m2: '20', p0: 10, path_m1: '30', path_m2: '20', path_p1: '20', path_x: NULL }]"

[[test.results]]
value = "[{ beyond: NULL, by_chain: 'bottom', by_path: 'bottom' }]"

[[test.results]]
value = "[{ name: 'obj_flat' }]"

[[test.results]]
value = "[{ name: 'absent' }, { name: 'arr_mixed' }, { name: 'arr_nested' }, { name: 'obj_arr' }, { name: 'obj_deep' }, { name: 'obj_nest' }, { name: 'scalar_bool' }, { name: 'scalar_null' }, { name: 'scalar_num' }, { name: 'scalar_str' }]"

[[test.results]]
value = "[{ name: 'obj_flat' }, { name: 'obj_keys' }]"

[[test.results]]
value = "[{ name: 'arr_mixed' }, { name: 'arr_nested' }]"

[[test.results]]
value = "[{ name: 'arr_mixed' }, { name: 'arr_nested' }]"

[[test.results]]
value = "[{ name: 'obj_flat' }]"

[[test.results]]
value = "[{ name: 'obj_flat' }]"

[[test.results]]
value = "[{ name: 'obj_flat' }]"

[[test.results]]
value = "[{ name: 'obj_arr' }, { name: 'obj_flat' }]"

[[test.results]]
value = "[{ n: 12 }]"

[[test.results]]
value = "[{ n: 12 }]"

[[test.results]]
value = "[{ n: 12 }]"
*/

-- A chain through an absent member, a JSON null or a scalar reaches nothing.
SELECT name, data -> 'x' -> 'y' AS v FROM jdoc ORDER BY name;

SELECT data -> 'f' ->> 'x' AS null_member, data -> 'missing' -> 0 AS absent, data -> 'g' ->> 'h' AS nested, data -> 'i' -> -1 AS last, data -> 'i' -> 0 ->> 'z' AS scalar FROM jdoc WHERE name = 'obj_flat';

SELECT name, data -> 0 ->> 'x' AS v FROM jdoc ORDER BY name;

SELECT data -> 3 ->> 'x' AS v FROM jdoc WHERE name = 'arr_mixed';

-- An element is counted from either end at any step of a chain, and a path starts from one.
SELECT data -> 'e' -> 'f' ->> -1 AS m1, data -> 'e' -> 'f' ->> -2 AS m2, data -> 'e' -> 'f' -> 0 AS p0, (data -> 'e') #>> '{f,-1}' AS path_m1, data -> 'e' #>> '{f,-2}' AS path_m2, data -> 'e' #>> '{f,1}' AS path_p1, data -> 'e' #>> '{f,x}' AS path_x FROM jdoc WHERE name = 'obj_deep';

SELECT data -> 'l1' -> 'l2' #>> '{l3,l4,l5,-1}' AS by_path, data -> 'l1' -> 'l2' -> 'l3' -> 'l4' -> 'l5' ->> 0 AS by_chain, data -> 'l1' -> 'l2' -> 'l3' -> 'l4' -> 'l5' -> 1 ->> 0 AS beyond FROM jdoc WHERE name = 'obj_nest';

SELECT name FROM jdoc WHERE data -> 'g' ->> 'h' = '1' ORDER BY name;

-- IS NULL and IS NOT NULL of a JSON operator.
SELECT name FROM jdoc WHERE data -> 'x' IS NULL ORDER BY name;

SELECT name FROM jdoc WHERE data -> 'x' IS NOT NULL ORDER BY name;

SELECT name FROM jdoc WHERE data -> 0 IS NOT NULL ORDER BY name;

SELECT name FROM jdoc WHERE data ->> -1 IS NOT NULL ORDER BY name;

SELECT name FROM jdoc WHERE data #>> '{g,h}' IS NOT NULL ORDER BY name;

SELECT name FROM jdoc WHERE data -> 'g' -> 'h' IS NOT NULL ORDER BY name;

SELECT name FROM jdoc WHERE data ->> 'f' IS NULL AND data ->> 'a' IS NOT NULL ORDER BY name;

SELECT name FROM jdoc WHERE data ->> 'g' IS NOT NULL OR data ->> 'arr' IS NOT NULL ORDER BY name;

SELECT count(*) AS n FROM jdoc WHERE data -> 'x' -> 'y' IS NULL;

SELECT count(*) AS n FROM jdoc WHERE data -> NULL -> 'x' IS NULL;

SELECT count(*) AS n FROM jdoc WHERE data #>> '{x,NULL}' IS NULL;
