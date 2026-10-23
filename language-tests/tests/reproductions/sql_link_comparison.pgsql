/**
[env]
namespace = true
database = true
imports = ["reproductions/sql_link_comparison_import.surql"]

[test]
reason = "SQL link comparisons preserve non-null values that name no record and compare integral numeric literals with integer keys."

[[test.results]]
value = "[{ id: 1 }, { id: 2 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: 1 }]"

[[test.results]]
value = "[{ id: 1 }]"

[[test.results]]
value = "[]"
*/

SELECT id FROM link_cmp WHERE dept <> '' ORDER BY id;
SELECT id FROM link_cmp WHERE dept <> lower('') ORDER BY id;
SELECT id FROM link_cmp WHERE dept IS NOT DISTINCT FROM lower('');
SELECT id FROM link_cmp WHERE dept = 1.0;
SELECT id FROM link_cmp WHERE dept IN (1.0);
SELECT id FROM link_cmp WHERE dept = 1.5;
