/**
[env]
namespace = true
database = true
imports = ["pgsql/_json_paths.surql"]

[test]
reason = "PostgreSQL #>> reads its path as the text array it is: the unquoted word NULL is a NULL element, which gives NULL, and a quoted or escaped one is a key; a backslash escapes the next character; a path takes every form of a step however many of them the path holds, and a non-empty path on a scalar gives NULL"

[[test.results]]
value = "[{ v: NULL }]"

[[test.results]]
value = "[{ v: NULL, w: NULL, x: NULL }]"

[[test.results]]
value = "[{ below: 'null-below', escaped: 'null-key', quoted: 'null-key' }]"

[[test.results]]
value = "[{ bs: 'backslash-key', comma: 'comma-key', quoted_bs: 'backslash-key', quoted_comma: 'comma-key' }]"

[[test.results]]
value = "[{ ab: 'ab', spaced: 'ab' }]"

[[test.results]]
value = "[{ v: 'y' }]"

[[test.results]]
value = "[{ v: 'x' }]"

[[test.results]]
value = "[{ v: 'y' }]"

[[test.results]]
value = "[{ v: NULL }]"

[[test.results]]
value = "[{ first: 'in-list', in_list: 'in-list', inner_v: 'inner', padded: 'padded' }]"

[[test.results]]
value = "[{ name: 'keys', v: NULL }, { name: 'minus', v: 'inner' }]"

[[test.results]]
value = "[{ deep: NULL, m1: NULL, mixed: NULL, name: 'number', z1: NULL }, { deep: NULL, m1: NULL, mixed: NULL, name: 'text', z1: NULL }]"
*/

SELECT data #>> '{NULL}' AS v FROM jpath WHERE name = 'keys';

SELECT data #>> '{null}' AS v, data #>> '{a,NuLL}' AS w, data #>> '{nk,NULL}' AS x FROM jpath WHERE name = 'keys';

SELECT data #>> '{"NULL"}' AS quoted, data #>> '{\NULL}' AS escaped, data #>> '{nk,"NULL"}' AS below FROM jpath WHERE name = 'keys';

SELECT data #>> '{a\\b}' AS bs, data #>> '{"a\\b"}' AS quoted_bs, data #>> '{a\,b}' AS comma, data #>> '{"a,b"}' AS quoted_comma FROM jpath WHERE name = 'keys';

SELECT data #>> '{a,b}' AS ab, data #>> '{ a , b }' AS spaced FROM jpath WHERE name = 'keys';

SELECT data #>> '{-1,-1,-1,-1,-1,-1}' AS v FROM jpath WHERE name = 'deep';

SELECT data #>> '{-1,-1,-1,-1,-1,-2}' AS v FROM jpath WHERE name = 'deep';

SELECT data #>> '{1,01,+1,1,1,01}' AS v FROM jpath WHERE name = 'deep';

SELECT data #>> '{1,-1,1,-1,1,-1,-1}' AS v FROM jpath WHERE name = 'deep';

SELECT data #>> '{-1,-1}' AS inner_v, data #>> '{-1,01}' AS padded, data #>> '{list,-1,-1}' AS in_list, data #>> '{list,0,-1}' AS first FROM jpath WHERE name = 'minus';

SELECT name, data #>> '{-1,-1}' AS v FROM jpath WHERE name IN ('keys', 'minus') ORDER BY name;

SELECT name, data #>> '{-1}' AS m1, data #>> '{01}' AS z1, data #>> '{-1,-1,-1,-1,-1}' AS deep, data #>> '{1,01,+1,-1,01}' AS mixed FROM jpath WHERE name IN ('text', 'number') ORDER BY name;
