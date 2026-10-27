/**
[env]
namespace = true
database = true
imports = ["pgsql/_json_paths.surql"]

[test]
reason = "PostgreSQL #> walks a text-array path and gives the JSON value it reaches, NULL where there is none; jsonb_extract_path and jsonb_extract_path_text take the steps of the path as arguments"

[[test.results]]
value = "[{ v: 'ab' }]"

[[test.results]]
value = "[{ v: 'y' }]"

[[test.results]]
value = "[{ v: [0, [0, [0, [0, ['x', 'y']]]]] }]"

[[test.results]]
value = "[{ v: NULL }]"

[[test.results]]
value = "[{ missing: true, present: false }]"

[[test.results]]
value = """[{ v: { "-1": 'in-list' } }]"""

[[test.results]]
value = "[{ v: 'ab' }]"

[[test.results]]
value = "[{ missing: NULL, v: 'ab' }]"

[[test.results]]
value = "[{ v: 'inner' }]"

*/

SELECT data #> '{a,b}' AS v FROM jpath WHERE name = 'keys';

SELECT data #> '{-1,-1,-1,-1,-1,-1}' AS v FROM jpath WHERE name = 'deep';

SELECT data #> '{-1}' AS v FROM jpath WHERE name = 'deep';

SELECT data #> '{zz}' AS v FROM jpath WHERE name = 'keys';

SELECT data #> '{zz}' IS NULL AS missing, data #> '{a}' IS NULL AS present FROM jpath WHERE name = 'keys';

SELECT data #> '{list,0}' AS v FROM jpath WHERE name = 'minus';

SELECT jsonb_extract_path(data, 'a', 'b') AS v FROM jpath WHERE name = 'keys';

SELECT jsonb_extract_path_text(data, 'a', 'b') AS v, jsonb_extract_path_text(data, 'a', 'zz') AS missing FROM jpath WHERE name = 'keys';

SELECT jsonb_extract_path_text(data, '-1', '-1') AS v FROM jpath WHERE name = 'minus';
