/**
[env]
namespace = true
database = true
imports = ["pgsql/_count_values.surql"]

[test]
reason = "COUNT(x) counts the rows whose x is neither NULL nor missing, whatever the value: 0, false, '' and [] are counted, a group with no such value counts 0, and the count composes with HAVING and FILTER"

[[test.results]]
value = "[{ n: 4, total: 7 }]"

[[test.results]]
value = "[{ grp: 'x', n: 3 }, { grp: 'y', n: 1 }, { grp: 'z', n: 0 }]"

[[test.results]]
value = "[{ grp: 'z' }]"

[[test.results]]
value = "[{ n: 0 }]"

[[test.results]]
value = "[{ n: 1 }]"
*/

SELECT count(v) AS n, count(*) AS total FROM cv_t;
SELECT grp, count(v) AS n FROM cv_t GROUP BY grp ORDER BY grp;
SELECT grp FROM cv_t GROUP BY grp HAVING count(v) = 0 ORDER BY grp;
SELECT count(v) AS n FROM cv_t WHERE grp = 'none';
SELECT count(v) FILTER (WHERE grp <> 'x') AS n FROM cv_t;
