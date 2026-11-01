/**
[env]
namespace = true
database = true

[test]
reason = "set_config answers with the value it is asked to set, as pg_dump's first query needs"

[[test.results]]
value = "[{ v: '' }]"

[[test.results]]
value = "[{ v: 'on' }]"
*/

SELECT pg_catalog.set_config('search_path', '', false) AS v;

SELECT set_config('standard_conforming_strings', 'on', true) AS v;
