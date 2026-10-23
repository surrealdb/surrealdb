/**
[env]
namespace = true
database = true
imports = ["pgsql/_json.surql", "pgsql/_json_shapes.surql"]

[test]
reason = "PostgreSQL JSON operators: ->> yields text, NULL for an absent or null value and JSON text for a non-string; #>> walks a text-array path; a field step reads only an object and an index step only an array, with a negative index counting from the end"

[[test.results]]
value = "[{ city: 'London', name: 'a' }, { city: 'Paris', name: 'b' }, { city: NULL, name: 'c' }]"

[[test.results]]
value = "[{ n: '3', ok: 'true' }]"

[[test.results]]
value = """[{ geo: '{"lat":51.5}', tags: '["x","y"]' }]"""

[[test.results]]
value = "[{ gone: NULL, missing: NULL }]"

[[test.results]]
value = "[{ lat: '51.5', lon: NULL, tag: 'y' }]"

[[test.results]]
value = "[{ name: 'a' }]"

[[test.results]]
value = "[{ tags: ['x', 'y'] }]"

[[test.results]]
value = "[{ name: 'absent', v: NULL }, { name: 'arr_mixed', v: NULL }, { name: 'arr_nested', v: NULL }, { name: 'obj_arr', v: NULL }, { name: 'obj_deep', v: NULL }, { name: 'obj_flat', v: 'flat-x' }, { name: 'obj_keys', v: 'ex' }, { name: 'obj_nest', v: NULL }, { name: 'scalar_bool', v: NULL }, { name: 'scalar_null', v: NULL }, { name: 'scalar_num', v: NULL }, { name: 'scalar_str', v: NULL }]"

[[test.results]]
value = "[{ name: 'obj_flat' }, { name: 'obj_keys' }]"

[[test.results]]
value = "[{ n: 2 }]"

[[test.results]]
value = "[{ name: 'absent', v: NULL }, { name: 'arr_mixed', v: NULL }, { name: 'arr_nested', v: NULL }, { name: 'obj_arr', v: NULL }, { name: 'obj_deep', v: NULL }, { name: 'obj_flat', v: 'flat-x' }, { name: 'obj_keys', v: 'ex' }, { name: 'obj_nest', v: NULL }, { name: 'scalar_bool', v: NULL }, { name: 'scalar_null', v: NULL }, { name: 'scalar_num', v: NULL }, { name: 'scalar_str', v: NULL }]"

[[test.results]]
value = "[{ g: { h: 1 }, i: [1, 2], zz: NULL }]"

[[test.results]]
value = "[{ name: 'arr_mixed', v: NULL }, { name: 'obj_arr', v: [{ x: 1 }, { x: 2 }] }]"

[[test.results]]
value = "[{ name: 'absent', v: NULL }, { name: 'arr_mixed', v: 'two' }, { name: 'obj_keys', v: NULL }, { name: 'scalar_str', v: NULL }]"

[[test.results]]
value = "[{ name: 'absent', v: NULL }, { name: 'arr_mixed', v: 'two' }, { name: 'arr_nested', v: [3, [4, 5]] }, { name: 'obj_arr', v: NULL }, { name: 'obj_deep', v: NULL }, { name: 'obj_flat', v: NULL }, { name: 'obj_keys', v: NULL }, { name: 'obj_nest', v: NULL }, { name: 'scalar_bool', v: NULL }, { name: 'scalar_null', v: NULL }, { name: 'scalar_num', v: NULL }, { name: 'scalar_str', v: NULL }]"

[[test.results]]
value = "[{ m2: NULL, m4: '1', m5: NULL, p0: '1', p1: 'two', p2: NULL, p4: NULL }]"

[[test.results]]
value = "[{ m1: { x: 1 }, m5: NULL, p3: { x: 1 }, p4: NULL }]"

[[test.results]]
value = "[{ name: 'absent', v: NULL }, { name: 'arr_mixed', v: { x: 1 } }, { name: 'arr_nested', v: { x: 'in-array' } }, { name: 'obj_arr', v: NULL }, { name: 'obj_deep', v: NULL }, { name: 'obj_flat', v: NULL }, { name: 'obj_keys', v: NULL }, { name: 'obj_nest', v: NULL }]"

[[test.results]]
value = "[{ k0: 'zero', k01: 'oh-one', k1: 'one', k2: NULL, km1: 'minus', kx: 'ex' }]"

[[test.results]]
value = "[{ i0: NULL, i1: NULL, im1: NULL, t0: 'zero', t01: 'oh-one', t1: 'one', tm1: 'minus' }]"

[[test.results]]
value = "[{ s0: '1', s01: 'two', s1: 'two', s2: NULL, s2x: NULL, s3x: '1', s4: NULL, sm1x: '1', sm5: NULL, sx: NULL }]"

[[test.results]]
value = "[{ arr0x: NULL, arr1x: NULL, arr2x: NULL, arr_x: NULL, name: 'absent' }, { arr0x: NULL, arr1x: NULL, arr2x: NULL, arr_x: NULL, name: 'arr_mixed' }, { arr0x: NULL, arr1x: NULL, arr2x: NULL, arr_x: NULL, name: 'arr_nested' }, { arr0x: '1', arr1x: '2', arr2x: NULL, arr_x: NULL, name: 'obj_arr' }, { arr0x: NULL, arr1x: NULL, arr2x: NULL, arr_x: NULL, name: 'obj_deep' }, { arr0x: NULL, arr1x: NULL, arr2x: NULL, arr_x: NULL, name: 'obj_flat' }, { arr0x: NULL, arr1x: NULL, arr2x: NULL, arr_x: NULL, name: 'obj_keys' }, { arr0x: NULL, arr1x: NULL, arr2x: NULL, arr_x: NULL, name: 'obj_nest' }, { arr0x: NULL, arr1x: NULL, arr2x: NULL, arr_x: NULL, name: 'scalar_bool' }, { arr0x: NULL, arr1x: NULL, arr2x: NULL, arr_x: NULL, name: 'scalar_null' }, { arr0x: NULL, arr1x: NULL, arr2x: NULL, arr_x: NULL, name: 'scalar_num' }, { arr0x: NULL, arr1x: NULL, arr2x: NULL, arr_x: NULL, name: 'scalar_str' }]"

[[test.results]]
value = "[{ f01: '20', f1: '20', f3: NULL, fm1: '30', fm3: '10', fm4: NULL, fx: NULL }]"

[[test.results]]
value = "[{ name: 'obj_deep' }]"

[[test.results]]
value = "[{ a: '1', a0: NULL, ax: NULL, b0: NULL, bx: NULL, c: 'true', cx: NULL, d: 'false', e: '0', f: NULL, f0: NULL, fx: NULL, gh: '1', gh0: NULL, ghx: NULL, i0: '1', i0x: NULL, i1: '2', i2: NULL, im1: '2', im2: '1', im3: NULL, ix: NULL, zzx: NULL }]"

[[test.results]]
value = "[{ name: 'absent', v: NULL }, { name: 'scalar_bool', v: 'true' }, { name: 'scalar_null', v: NULL }, { name: 'scalar_num', v: '42' }, { name: 'scalar_str', v: 'hello' }]"

[[test.results]]
value = "[{ a01: 2, a110: 4, a1m1: [4, 5], a2: { x: 'in-array' }, p00: '1', p0x: NULL, p110: '4', p11m1: '5', p2x: 'in-array', p2x0: NULL }]"

[[test.results]]
value = "[{ beyond: NULL, first: 'bottom', l3: { l4: { l5: ['bottom'] } }, l5: ['bottom'], last: 'bottom', missing: NULL }]"

[[test.results]]
value = "[{ a: NULL, b: NULL }]"

[[test.results]]
value = "[{ name: 'arr_mixed', v: NULL }, { name: 'obj_flat', v: 'flat-x' }, { name: 'obj_keys', v: 'ex' }]"

[[test.results]]
value = "[{ v: 'two' }]"

[[test.results]]
value = "[{ by_idx: 'two', by_key: NULL, name: 'arr_mixed' }, { by_idx: NULL, by_key: 'flat-x', name: 'obj_flat' }, { by_idx: NULL, by_key: NULL, name: 'obj_keys' }]"

[[test.results]]
value = "[{ name: 'arr_mixed', v: NULL }, { name: 'obj_flat', v: 'flat-x' }]"
*/

SELECT name, data ->> 'city' AS city FROM doc ORDER BY name;

SELECT data ->> 'n' AS n, data ->> 'ok' AS ok FROM doc WHERE name = 'a';

SELECT data ->> 'tags' AS tags, data ->> 'geo' AS geo FROM doc WHERE name = 'a';

SELECT data ->> 'missing' AS missing, data ->> 'gone' AS gone FROM doc WHERE name = 'a';

SELECT data #>> '{geo,lat}' AS lat, data #>> '{tags,1}' AS tag, data #>> '{geo,lon}' AS lon FROM doc WHERE name = 'a';

SELECT name FROM doc WHERE data ->> 'city' = 'London';

SELECT data -> 'tags' AS tags FROM doc WHERE name = 'a';

-- A field step reads an object and nothing else: an array, a scalar, a JSON null and an absent column give NULL.
SELECT name, data ->> 'x' AS v FROM jdoc ORDER BY name;

SELECT name FROM jdoc WHERE data ->> 'x' IS NOT NULL ORDER BY name;

SELECT count(data ->> 'x') AS n FROM jdoc;

SELECT name, data -> 'x' AS v FROM jdoc ORDER BY name;

SELECT data -> 'g' AS g, data -> 'i' AS i, data -> 'zz' AS zz FROM jdoc WHERE name = 'obj_flat';

SELECT name, data -> 'arr' AS v FROM jdoc WHERE name IN ('obj_arr', 'arr_mixed') ORDER BY name;

-- An integer operand indexes an array and nothing else.
SELECT name, data ->> 1 AS v FROM jdoc WHERE name IN ('arr_mixed', 'obj_keys', 'scalar_str', 'absent') ORDER BY name;

SELECT name, data -> 1 AS v FROM jdoc ORDER BY name;

SELECT data ->> 0 AS p0, data ->> 1 AS p1, data ->> 2 AS p2, data ->> 4 AS p4, data ->> -2 AS m2, data ->> -4 AS m4, data ->> -5 AS m5 FROM jdoc WHERE name = 'arr_mixed';

SELECT data -> 3 AS p3, data -> -1 AS m1, data -> 4 AS p4, data -> -5 AS m5 FROM jdoc WHERE name = 'arr_mixed';

SELECT name, data -> -1 AS v FROM jdoc WHERE name NOT LIKE 'scalar%' ORDER BY name;

-- A path step names a member of an object, even when it spells an integer, and indexes an array when it does.
SELECT data #>> '{0}' AS k0, data #>> '{1}' AS k1, data #>> '{-1}' AS km1, data #>> '{01}' AS k01, data #>> '{2}' AS k2, data #>> '{x}' AS kx FROM jdoc WHERE name = 'obj_keys';

SELECT data ->> '0' AS t0, data ->> '1' AS t1, data ->> '-1' AS tm1, data ->> '01' AS t01, data ->> 1 AS i1, data ->> 0 AS i0, data -> -1 AS im1 FROM jdoc WHERE name = 'obj_keys';

SELECT data #>> '{0}' AS s0, data #>> '{01}' AS s01, data #>> '{1}' AS s1, data #>> '{3,x}' AS s3x, data #>> '{-1,x}' AS sm1x, data #>> '{2}' AS s2, data #>> '{2,x}' AS s2x, data #>> '{x}' AS sx, data #>> '{4}' AS s4, data #>> '{-5}' AS sm5 FROM jdoc WHERE name = 'arr_mixed';

SELECT name, data #>> '{arr,x}' AS arr_x, data #>> '{arr,0,x}' AS arr0x, data #>> '{arr,1,x}' AS arr1x, data #>> '{arr,2,x}' AS arr2x FROM jdoc ORDER BY name;

SELECT data #>> '{e,f,x}' AS fx, data #>> '{e,f,1}' AS f1, data #>> '{e,f,01}' AS f01, data #>> '{e,f,-1}' AS fm1, data #>> '{e,f,-3}' AS fm3, data #>> '{e,f,-4}' AS fm4, data #>> '{e,f,3}' AS f3 FROM jdoc WHERE name = 'obj_deep';

SELECT name FROM jdoc WHERE data #>> '{e,f,-1}' = '30';

-- A scalar or a JSON null at any position of a path reaches nothing further.
SELECT data #>> '{a}' AS a, data #>> '{a,x}' AS ax, data #>> '{a,0}' AS a0, data #>> '{b,0}' AS b0, data #>> '{b,x}' AS bx, data #>> '{c}' AS c, data #>> '{c,x}' AS cx, data #>> '{d}' AS d, data #>> '{e}' AS e, data #>> '{f}' AS f, data #>> '{f,x}' AS fx, data #>> '{f,0}' AS f0, data #>> '{g,h}' AS gh, data #>> '{g,h,x}' AS ghx, data #>> '{g,h,0}' AS gh0, data #>> '{i,0}' AS i0, data #>> '{i,1}' AS i1, data #>> '{i,2}' AS i2, data #>> '{i,-1}' AS im1, data #>> '{i,-2}' AS im2, data #>> '{i,-3}' AS im3, data #>> '{i,x}' AS ix, data #>> '{i,0,x}' AS i0x, data #>> '{zz,x}' AS zzx FROM jdoc WHERE name = 'obj_flat';

SELECT name, data #>> '{}' AS v FROM jdoc WHERE name LIKE 'scalar%' OR name = 'absent' ORDER BY name;

-- Arrays of arrays, objects in arrays and deep paths.
SELECT data -> 0 -> 1 AS a01, data -> 1 -> 1 -> 0 AS a110, data -> 1 -> -1 AS a1m1, data -> 2 AS a2, data #>> '{0,0}' AS p00, data #>> '{1,1,0}' AS p110, data #>> '{1,1,-1}' AS p11m1, data #>> '{0,x}' AS p0x, data #>> '{2,x}' AS p2x, data #>> '{2,x,0}' AS p2x0 FROM jdoc WHERE name = 'arr_nested';

SELECT data #>> '{l1,l2,l3,l4,l5,0}' AS first, data #>> '{l1,l2,l3,l4,l5,-1}' AS last, data #>> '{l1,l2,l3,l4,l5,1}' AS beyond, data #>> '{l1,l2,l3,l4,x,0}' AS missing, data -> 'l1' -> 'l2' -> 'l3' AS l3, data -> 'l1' -> 'l2' -> 'l3' -> 'l4' -> 'l5' AS l5 FROM jdoc WHERE name = 'obj_nest';

-- A NULL operand, operands that are not literals, and the operators in a join.
SELECT data -> NULL AS a, data ->> NULL AS b FROM jdoc WHERE name = 'obj_flat';

SELECT name, data ->> lower('X') AS v FROM jdoc WHERE name IN ('obj_flat', 'obj_keys', 'arr_mixed') ORDER BY name;

SELECT data ->> (0 + 1) AS v FROM jdoc WHERE name = 'arr_mixed';

SELECT name, data ->> key AS by_key, data ->> idx AS by_idx FROM jdoc WHERE name IN ('arr_mixed', 'obj_flat', 'obj_keys') ORDER BY name;

SELECT a.name, b.data ->> 'x' AS v FROM jdoc a JOIN jdoc b ON b.name = a.name WHERE a.name IN ('obj_flat', 'arr_mixed') ORDER BY a.name;
