/**
[env]
namespace = true
database = true

[test]
reason = "A grouped aggregate over math::sum, min, max, mean, median, stddev, variance (and count) skips every value that is not a number, in every group, as a plain SELECT does. A materialized view of the same aggregate must hold what its SELECT computes after every write, and a GROUP ALL view keeps its one row, including when no record is left."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, k: 1, x: 'a' }]"

[[test.results]]
value = "[{ k: 1, lo: Infinity, s: 0 }]"

[[test.results]]
value = "[{ hi: -Infinity, lo: Infinity, m: NaN, s: 0 }]"

[[test.results]]
value = "[{ id: t:2, k: 1, x: 2 }]"

[[test.results]]
value = "[{ hi: 2, k: 1, lo: 2, s: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ grp: 'c', id: m:1, x: 'a' }]"

[[test.results]]
value = "[{ grp: 'c', id: m:2, x: 4 }]"

[[test.results]]
value = "[{ grp: 'd', id: m:3, x: true }]"

[[test.results]]
value = "[{ grp: 'c', lo: 4, md: 4, s: 4 }, { grp: 'd', lo: Infinity, md: NONE, s: 0 }]"

[[test.results]]
value = "[{ md: 4 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_sum:1, x: 1 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_sum:2 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_sum:3, x: 'a' }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_sum:4, x: [1, 2] }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_sum:5, x: false }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_sum:6, x: NULL }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_sum:7, x: 4 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_sum:1, x: 1 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_sum:3, x: 7 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_sum:7, x: 'b' }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_sum:4, x: 10 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_sum:4, x: {  } }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_sum:5, x: false }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_min:1, x: 1 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_min:2 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_min:3, x: 'a' }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_min:4, x: [1, 2] }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_min:5, x: false }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_min:6, x: NULL }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_min:7, x: 4 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_min:1, x: 1 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_min:3, x: 7 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_min:7, x: 'b' }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_min:4, x: 10 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_min:4, x: {  } }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_min:5, x: false }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_max:1, x: 1 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_max:2 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_max:3, x: 'a' }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_max:4, x: [1, 2] }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_max:5, x: false }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_max:6, x: NULL }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_max:7, x: 4 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_max:1, x: 1 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_max:3, x: 7 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_max:7, x: 'b' }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_max:4, x: 10 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_max:4, x: {  } }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_max:5, x: false }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_mean:1, x: 1 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_mean:2 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_mean:3, x: 'a' }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_mean:4, x: [1, 2] }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_mean:5, x: false }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_mean:6, x: NULL }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_mean:7, x: 4 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_mean:1, x: 1 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_mean:3, x: 7 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_mean:7, x: 'b' }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_mean:4, x: 10 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_mean:4, x: {  } }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_mean:5, x: false }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_stddev:1, x: 1 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_stddev:2 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_stddev:3, x: 'a' }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_stddev:4, x: [1, 2] }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_stddev:5, x: false }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_stddev:6, x: NULL }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_stddev:7, x: 4 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_stddev:1, x: 1 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_stddev:3, x: 7 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_stddev:7, x: 'b' }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_stddev:4, x: 10 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_stddev:4, x: {  } }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_stddev:5, x: false }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_variance:1, x: 1 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_variance:2 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_variance:3, x: 'a' }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_variance:4, x: [1, 2] }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_variance:5, x: false }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_variance:6, x: NULL }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_variance:7, x: 4 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_variance:1, x: 1 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_variance:3, x: 7 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_variance:7, x: 'b' }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_variance:4, x: 10 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_variance:4, x: {  } }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_variance:5, x: false }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_countx:1, x: 1 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_countx:2 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_countx:3, x: 'a' }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_countx:4, x: [1, 2] }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_countx:5, x: false }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_countx:6, x: NULL }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_countx:7, x: 4 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_countx:1, x: 1 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_countx:3, x: 7 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'd', id: s_countx:7, x: 'b' }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_countx:4, x: 10 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_countx:4, x: {  } }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ grp: 'c', id: s_countx:5, x: false }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/

DEFINE TABLE t;
CREATE t:1 SET k = 1, x = 'a';
SELECT k, math::sum(x) AS s, math::min(x) AS lo FROM t GROUP BY k;
SELECT math::sum(x) AS s, math::min(x) AS lo, math::max(x) AS hi, math::mean(x) AS m FROM t GROUP ALL;
CREATE t:2 SET k = 1, x = 2;
SELECT k, math::sum(x) AS s, math::min(x) AS lo, math::max(x) AS hi FROM t GROUP BY k;
REMOVE TABLE t;
DEFINE TABLE m;
CREATE m:1 SET grp = 'c', x = 'a';
CREATE m:2 SET grp = 'c', x = 4;
CREATE m:3 SET grp = 'd', x = true;
SELECT grp, math::median(x) AS md, math::sum(x) AS s, math::min(x) AS lo FROM m GROUP BY grp;
SELECT math::median(x) AS md FROM m GROUP ALL;
REMOVE TABLE m;

-- math::sum(x): view == recompute after each write, GROUP BY and GROUP ALL
DEFINE TABLE s_sum;
DEFINE TABLE v_sum AS SELECT grp, math::sum(x) AS a FROM s_sum GROUP BY grp;
DEFINE TABLE e_sum AS SELECT math::sum(x) AS a FROM s_sum GROUP ALL;
RETURN (<string>(SELECT grp, a FROM v_sum ORDER BY grp) == <string>(SELECT grp, math::sum(x) AS a FROM s_sum GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_sum) == <string>(SELECT VALUE a FROM (SELECT math::sum(x) AS a FROM s_sum GROUP ALL)));
CREATE s_sum:1 SET grp = 'c', x = 1;
RETURN (<string>(SELECT grp, a FROM v_sum ORDER BY grp) == <string>(SELECT grp, math::sum(x) AS a FROM s_sum GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_sum) == <string>(SELECT VALUE a FROM (SELECT math::sum(x) AS a FROM s_sum GROUP ALL)));
CREATE s_sum:2 SET grp = 'c';
RETURN (<string>(SELECT grp, a FROM v_sum ORDER BY grp) == <string>(SELECT grp, math::sum(x) AS a FROM s_sum GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_sum) == <string>(SELECT VALUE a FROM (SELECT math::sum(x) AS a FROM s_sum GROUP ALL)));
CREATE s_sum:3 SET grp = 'c', x = 'a';
RETURN (<string>(SELECT grp, a FROM v_sum ORDER BY grp) == <string>(SELECT grp, math::sum(x) AS a FROM s_sum GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_sum) == <string>(SELECT VALUE a FROM (SELECT math::sum(x) AS a FROM s_sum GROUP ALL)));
CREATE s_sum:4 SET grp = 'c', x = [1, 2];
RETURN (<string>(SELECT grp, a FROM v_sum ORDER BY grp) == <string>(SELECT grp, math::sum(x) AS a FROM s_sum GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_sum) == <string>(SELECT VALUE a FROM (SELECT math::sum(x) AS a FROM s_sum GROUP ALL)));
CREATE s_sum:5 SET grp = 'd', x = false;
RETURN (<string>(SELECT grp, a FROM v_sum ORDER BY grp) == <string>(SELECT grp, math::sum(x) AS a FROM s_sum GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_sum) == <string>(SELECT VALUE a FROM (SELECT math::sum(x) AS a FROM s_sum GROUP ALL)));
CREATE s_sum:6 SET grp = 'd', x = NULL;
RETURN (<string>(SELECT grp, a FROM v_sum ORDER BY grp) == <string>(SELECT grp, math::sum(x) AS a FROM s_sum GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_sum) == <string>(SELECT VALUE a FROM (SELECT math::sum(x) AS a FROM s_sum GROUP ALL)));
CREATE s_sum:7 SET grp = 'd', x = 4;
RETURN (<string>(SELECT grp, a FROM v_sum ORDER BY grp) == <string>(SELECT grp, math::sum(x) AS a FROM s_sum GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_sum) == <string>(SELECT VALUE a FROM (SELECT math::sum(x) AS a FROM s_sum GROUP ALL)));
UPDATE s_sum:1 SET grp = 'd';
RETURN (<string>(SELECT grp, a FROM v_sum ORDER BY grp) == <string>(SELECT grp, math::sum(x) AS a FROM s_sum GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_sum) == <string>(SELECT VALUE a FROM (SELECT math::sum(x) AS a FROM s_sum GROUP ALL)));
UPDATE s_sum:3 SET x = 7;
RETURN (<string>(SELECT grp, a FROM v_sum ORDER BY grp) == <string>(SELECT grp, math::sum(x) AS a FROM s_sum GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_sum) == <string>(SELECT VALUE a FROM (SELECT math::sum(x) AS a FROM s_sum GROUP ALL)));
UPDATE s_sum:7 SET x = 'b';
RETURN (<string>(SELECT grp, a FROM v_sum ORDER BY grp) == <string>(SELECT grp, math::sum(x) AS a FROM s_sum GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_sum) == <string>(SELECT VALUE a FROM (SELECT math::sum(x) AS a FROM s_sum GROUP ALL)));
UPDATE s_sum:4 SET x = 10;
RETURN (<string>(SELECT grp, a FROM v_sum ORDER BY grp) == <string>(SELECT grp, math::sum(x) AS a FROM s_sum GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_sum) == <string>(SELECT VALUE a FROM (SELECT math::sum(x) AS a FROM s_sum GROUP ALL)));
UPDATE s_sum:4 SET x = {};
RETURN (<string>(SELECT grp, a FROM v_sum ORDER BY grp) == <string>(SELECT grp, math::sum(x) AS a FROM s_sum GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_sum) == <string>(SELECT VALUE a FROM (SELECT math::sum(x) AS a FROM s_sum GROUP ALL)));
UPDATE s_sum:5 SET grp = 'c';
RETURN (<string>(SELECT grp, a FROM v_sum ORDER BY grp) == <string>(SELECT grp, math::sum(x) AS a FROM s_sum GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_sum) == <string>(SELECT VALUE a FROM (SELECT math::sum(x) AS a FROM s_sum GROUP ALL)));
DELETE s_sum:4;
RETURN (<string>(SELECT grp, a FROM v_sum ORDER BY grp) == <string>(SELECT grp, math::sum(x) AS a FROM s_sum GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_sum) == <string>(SELECT VALUE a FROM (SELECT math::sum(x) AS a FROM s_sum GROUP ALL)));
DELETE s_sum:7;
RETURN (<string>(SELECT grp, a FROM v_sum ORDER BY grp) == <string>(SELECT grp, math::sum(x) AS a FROM s_sum GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_sum) == <string>(SELECT VALUE a FROM (SELECT math::sum(x) AS a FROM s_sum GROUP ALL)));
DELETE s_sum:1;
RETURN (<string>(SELECT grp, a FROM v_sum ORDER BY grp) == <string>(SELECT grp, math::sum(x) AS a FROM s_sum GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_sum) == <string>(SELECT VALUE a FROM (SELECT math::sum(x) AS a FROM s_sum GROUP ALL)));
DELETE s_sum:2;
RETURN (<string>(SELECT grp, a FROM v_sum ORDER BY grp) == <string>(SELECT grp, math::sum(x) AS a FROM s_sum GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_sum) == <string>(SELECT VALUE a FROM (SELECT math::sum(x) AS a FROM s_sum GROUP ALL)));
DELETE s_sum:3;
RETURN (<string>(SELECT grp, a FROM v_sum ORDER BY grp) == <string>(SELECT grp, math::sum(x) AS a FROM s_sum GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_sum) == <string>(SELECT VALUE a FROM (SELECT math::sum(x) AS a FROM s_sum GROUP ALL)));
DELETE s_sum:5;
RETURN (<string>(SELECT grp, a FROM v_sum ORDER BY grp) == <string>(SELECT grp, math::sum(x) AS a FROM s_sum GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_sum) == <string>(SELECT VALUE a FROM (SELECT math::sum(x) AS a FROM s_sum GROUP ALL)));
DELETE s_sum:6;
RETURN (<string>(SELECT grp, a FROM v_sum ORDER BY grp) == <string>(SELECT grp, math::sum(x) AS a FROM s_sum GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_sum) == <string>(SELECT VALUE a FROM (SELECT math::sum(x) AS a FROM s_sum GROUP ALL)));
REMOVE TABLE e_sum;
REMOVE TABLE v_sum;
REMOVE TABLE s_sum;

-- math::min(x): view == recompute after each write, GROUP BY and GROUP ALL
DEFINE TABLE s_min;
DEFINE TABLE v_min AS SELECT grp, math::min(x) AS a FROM s_min GROUP BY grp;
DEFINE TABLE e_min AS SELECT math::min(x) AS a FROM s_min GROUP ALL;
RETURN (<string>(SELECT grp, a FROM v_min ORDER BY grp) == <string>(SELECT grp, math::min(x) AS a FROM s_min GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_min) == <string>(SELECT VALUE a FROM (SELECT math::min(x) AS a FROM s_min GROUP ALL)));
CREATE s_min:1 SET grp = 'c', x = 1;
RETURN (<string>(SELECT grp, a FROM v_min ORDER BY grp) == <string>(SELECT grp, math::min(x) AS a FROM s_min GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_min) == <string>(SELECT VALUE a FROM (SELECT math::min(x) AS a FROM s_min GROUP ALL)));
CREATE s_min:2 SET grp = 'c';
RETURN (<string>(SELECT grp, a FROM v_min ORDER BY grp) == <string>(SELECT grp, math::min(x) AS a FROM s_min GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_min) == <string>(SELECT VALUE a FROM (SELECT math::min(x) AS a FROM s_min GROUP ALL)));
CREATE s_min:3 SET grp = 'c', x = 'a';
RETURN (<string>(SELECT grp, a FROM v_min ORDER BY grp) == <string>(SELECT grp, math::min(x) AS a FROM s_min GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_min) == <string>(SELECT VALUE a FROM (SELECT math::min(x) AS a FROM s_min GROUP ALL)));
CREATE s_min:4 SET grp = 'c', x = [1, 2];
RETURN (<string>(SELECT grp, a FROM v_min ORDER BY grp) == <string>(SELECT grp, math::min(x) AS a FROM s_min GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_min) == <string>(SELECT VALUE a FROM (SELECT math::min(x) AS a FROM s_min GROUP ALL)));
CREATE s_min:5 SET grp = 'd', x = false;
RETURN (<string>(SELECT grp, a FROM v_min ORDER BY grp) == <string>(SELECT grp, math::min(x) AS a FROM s_min GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_min) == <string>(SELECT VALUE a FROM (SELECT math::min(x) AS a FROM s_min GROUP ALL)));
CREATE s_min:6 SET grp = 'd', x = NULL;
RETURN (<string>(SELECT grp, a FROM v_min ORDER BY grp) == <string>(SELECT grp, math::min(x) AS a FROM s_min GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_min) == <string>(SELECT VALUE a FROM (SELECT math::min(x) AS a FROM s_min GROUP ALL)));
CREATE s_min:7 SET grp = 'd', x = 4;
RETURN (<string>(SELECT grp, a FROM v_min ORDER BY grp) == <string>(SELECT grp, math::min(x) AS a FROM s_min GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_min) == <string>(SELECT VALUE a FROM (SELECT math::min(x) AS a FROM s_min GROUP ALL)));
UPDATE s_min:1 SET grp = 'd';
RETURN (<string>(SELECT grp, a FROM v_min ORDER BY grp) == <string>(SELECT grp, math::min(x) AS a FROM s_min GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_min) == <string>(SELECT VALUE a FROM (SELECT math::min(x) AS a FROM s_min GROUP ALL)));
UPDATE s_min:3 SET x = 7;
RETURN (<string>(SELECT grp, a FROM v_min ORDER BY grp) == <string>(SELECT grp, math::min(x) AS a FROM s_min GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_min) == <string>(SELECT VALUE a FROM (SELECT math::min(x) AS a FROM s_min GROUP ALL)));
UPDATE s_min:7 SET x = 'b';
RETURN (<string>(SELECT grp, a FROM v_min ORDER BY grp) == <string>(SELECT grp, math::min(x) AS a FROM s_min GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_min) == <string>(SELECT VALUE a FROM (SELECT math::min(x) AS a FROM s_min GROUP ALL)));
UPDATE s_min:4 SET x = 10;
RETURN (<string>(SELECT grp, a FROM v_min ORDER BY grp) == <string>(SELECT grp, math::min(x) AS a FROM s_min GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_min) == <string>(SELECT VALUE a FROM (SELECT math::min(x) AS a FROM s_min GROUP ALL)));
UPDATE s_min:4 SET x = {};
RETURN (<string>(SELECT grp, a FROM v_min ORDER BY grp) == <string>(SELECT grp, math::min(x) AS a FROM s_min GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_min) == <string>(SELECT VALUE a FROM (SELECT math::min(x) AS a FROM s_min GROUP ALL)));
UPDATE s_min:5 SET grp = 'c';
RETURN (<string>(SELECT grp, a FROM v_min ORDER BY grp) == <string>(SELECT grp, math::min(x) AS a FROM s_min GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_min) == <string>(SELECT VALUE a FROM (SELECT math::min(x) AS a FROM s_min GROUP ALL)));
DELETE s_min:4;
RETURN (<string>(SELECT grp, a FROM v_min ORDER BY grp) == <string>(SELECT grp, math::min(x) AS a FROM s_min GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_min) == <string>(SELECT VALUE a FROM (SELECT math::min(x) AS a FROM s_min GROUP ALL)));
DELETE s_min:7;
RETURN (<string>(SELECT grp, a FROM v_min ORDER BY grp) == <string>(SELECT grp, math::min(x) AS a FROM s_min GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_min) == <string>(SELECT VALUE a FROM (SELECT math::min(x) AS a FROM s_min GROUP ALL)));
DELETE s_min:1;
RETURN (<string>(SELECT grp, a FROM v_min ORDER BY grp) == <string>(SELECT grp, math::min(x) AS a FROM s_min GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_min) == <string>(SELECT VALUE a FROM (SELECT math::min(x) AS a FROM s_min GROUP ALL)));
DELETE s_min:2;
RETURN (<string>(SELECT grp, a FROM v_min ORDER BY grp) == <string>(SELECT grp, math::min(x) AS a FROM s_min GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_min) == <string>(SELECT VALUE a FROM (SELECT math::min(x) AS a FROM s_min GROUP ALL)));
DELETE s_min:3;
RETURN (<string>(SELECT grp, a FROM v_min ORDER BY grp) == <string>(SELECT grp, math::min(x) AS a FROM s_min GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_min) == <string>(SELECT VALUE a FROM (SELECT math::min(x) AS a FROM s_min GROUP ALL)));
DELETE s_min:5;
RETURN (<string>(SELECT grp, a FROM v_min ORDER BY grp) == <string>(SELECT grp, math::min(x) AS a FROM s_min GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_min) == <string>(SELECT VALUE a FROM (SELECT math::min(x) AS a FROM s_min GROUP ALL)));
DELETE s_min:6;
RETURN (<string>(SELECT grp, a FROM v_min ORDER BY grp) == <string>(SELECT grp, math::min(x) AS a FROM s_min GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_min) == <string>(SELECT VALUE a FROM (SELECT math::min(x) AS a FROM s_min GROUP ALL)));
REMOVE TABLE e_min;
REMOVE TABLE v_min;
REMOVE TABLE s_min;

-- math::max(x): view == recompute after each write, GROUP BY and GROUP ALL
DEFINE TABLE s_max;
DEFINE TABLE v_max AS SELECT grp, math::max(x) AS a FROM s_max GROUP BY grp;
DEFINE TABLE e_max AS SELECT math::max(x) AS a FROM s_max GROUP ALL;
RETURN (<string>(SELECT grp, a FROM v_max ORDER BY grp) == <string>(SELECT grp, math::max(x) AS a FROM s_max GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_max) == <string>(SELECT VALUE a FROM (SELECT math::max(x) AS a FROM s_max GROUP ALL)));
CREATE s_max:1 SET grp = 'c', x = 1;
RETURN (<string>(SELECT grp, a FROM v_max ORDER BY grp) == <string>(SELECT grp, math::max(x) AS a FROM s_max GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_max) == <string>(SELECT VALUE a FROM (SELECT math::max(x) AS a FROM s_max GROUP ALL)));
CREATE s_max:2 SET grp = 'c';
RETURN (<string>(SELECT grp, a FROM v_max ORDER BY grp) == <string>(SELECT grp, math::max(x) AS a FROM s_max GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_max) == <string>(SELECT VALUE a FROM (SELECT math::max(x) AS a FROM s_max GROUP ALL)));
CREATE s_max:3 SET grp = 'c', x = 'a';
RETURN (<string>(SELECT grp, a FROM v_max ORDER BY grp) == <string>(SELECT grp, math::max(x) AS a FROM s_max GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_max) == <string>(SELECT VALUE a FROM (SELECT math::max(x) AS a FROM s_max GROUP ALL)));
CREATE s_max:4 SET grp = 'c', x = [1, 2];
RETURN (<string>(SELECT grp, a FROM v_max ORDER BY grp) == <string>(SELECT grp, math::max(x) AS a FROM s_max GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_max) == <string>(SELECT VALUE a FROM (SELECT math::max(x) AS a FROM s_max GROUP ALL)));
CREATE s_max:5 SET grp = 'd', x = false;
RETURN (<string>(SELECT grp, a FROM v_max ORDER BY grp) == <string>(SELECT grp, math::max(x) AS a FROM s_max GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_max) == <string>(SELECT VALUE a FROM (SELECT math::max(x) AS a FROM s_max GROUP ALL)));
CREATE s_max:6 SET grp = 'd', x = NULL;
RETURN (<string>(SELECT grp, a FROM v_max ORDER BY grp) == <string>(SELECT grp, math::max(x) AS a FROM s_max GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_max) == <string>(SELECT VALUE a FROM (SELECT math::max(x) AS a FROM s_max GROUP ALL)));
CREATE s_max:7 SET grp = 'd', x = 4;
RETURN (<string>(SELECT grp, a FROM v_max ORDER BY grp) == <string>(SELECT grp, math::max(x) AS a FROM s_max GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_max) == <string>(SELECT VALUE a FROM (SELECT math::max(x) AS a FROM s_max GROUP ALL)));
UPDATE s_max:1 SET grp = 'd';
RETURN (<string>(SELECT grp, a FROM v_max ORDER BY grp) == <string>(SELECT grp, math::max(x) AS a FROM s_max GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_max) == <string>(SELECT VALUE a FROM (SELECT math::max(x) AS a FROM s_max GROUP ALL)));
UPDATE s_max:3 SET x = 7;
RETURN (<string>(SELECT grp, a FROM v_max ORDER BY grp) == <string>(SELECT grp, math::max(x) AS a FROM s_max GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_max) == <string>(SELECT VALUE a FROM (SELECT math::max(x) AS a FROM s_max GROUP ALL)));
UPDATE s_max:7 SET x = 'b';
RETURN (<string>(SELECT grp, a FROM v_max ORDER BY grp) == <string>(SELECT grp, math::max(x) AS a FROM s_max GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_max) == <string>(SELECT VALUE a FROM (SELECT math::max(x) AS a FROM s_max GROUP ALL)));
UPDATE s_max:4 SET x = 10;
RETURN (<string>(SELECT grp, a FROM v_max ORDER BY grp) == <string>(SELECT grp, math::max(x) AS a FROM s_max GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_max) == <string>(SELECT VALUE a FROM (SELECT math::max(x) AS a FROM s_max GROUP ALL)));
UPDATE s_max:4 SET x = {};
RETURN (<string>(SELECT grp, a FROM v_max ORDER BY grp) == <string>(SELECT grp, math::max(x) AS a FROM s_max GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_max) == <string>(SELECT VALUE a FROM (SELECT math::max(x) AS a FROM s_max GROUP ALL)));
UPDATE s_max:5 SET grp = 'c';
RETURN (<string>(SELECT grp, a FROM v_max ORDER BY grp) == <string>(SELECT grp, math::max(x) AS a FROM s_max GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_max) == <string>(SELECT VALUE a FROM (SELECT math::max(x) AS a FROM s_max GROUP ALL)));
DELETE s_max:4;
RETURN (<string>(SELECT grp, a FROM v_max ORDER BY grp) == <string>(SELECT grp, math::max(x) AS a FROM s_max GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_max) == <string>(SELECT VALUE a FROM (SELECT math::max(x) AS a FROM s_max GROUP ALL)));
DELETE s_max:7;
RETURN (<string>(SELECT grp, a FROM v_max ORDER BY grp) == <string>(SELECT grp, math::max(x) AS a FROM s_max GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_max) == <string>(SELECT VALUE a FROM (SELECT math::max(x) AS a FROM s_max GROUP ALL)));
DELETE s_max:1;
RETURN (<string>(SELECT grp, a FROM v_max ORDER BY grp) == <string>(SELECT grp, math::max(x) AS a FROM s_max GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_max) == <string>(SELECT VALUE a FROM (SELECT math::max(x) AS a FROM s_max GROUP ALL)));
DELETE s_max:2;
RETURN (<string>(SELECT grp, a FROM v_max ORDER BY grp) == <string>(SELECT grp, math::max(x) AS a FROM s_max GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_max) == <string>(SELECT VALUE a FROM (SELECT math::max(x) AS a FROM s_max GROUP ALL)));
DELETE s_max:3;
RETURN (<string>(SELECT grp, a FROM v_max ORDER BY grp) == <string>(SELECT grp, math::max(x) AS a FROM s_max GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_max) == <string>(SELECT VALUE a FROM (SELECT math::max(x) AS a FROM s_max GROUP ALL)));
DELETE s_max:5;
RETURN (<string>(SELECT grp, a FROM v_max ORDER BY grp) == <string>(SELECT grp, math::max(x) AS a FROM s_max GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_max) == <string>(SELECT VALUE a FROM (SELECT math::max(x) AS a FROM s_max GROUP ALL)));
DELETE s_max:6;
RETURN (<string>(SELECT grp, a FROM v_max ORDER BY grp) == <string>(SELECT grp, math::max(x) AS a FROM s_max GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_max) == <string>(SELECT VALUE a FROM (SELECT math::max(x) AS a FROM s_max GROUP ALL)));
REMOVE TABLE e_max;
REMOVE TABLE v_max;
REMOVE TABLE s_max;

-- math::mean(x): view == recompute after each write, GROUP BY and GROUP ALL
DEFINE TABLE s_mean;
DEFINE TABLE v_mean AS SELECT grp, math::mean(x) AS a FROM s_mean GROUP BY grp;
DEFINE TABLE e_mean AS SELECT math::mean(x) AS a FROM s_mean GROUP ALL;
RETURN (<string>(SELECT grp, a FROM v_mean ORDER BY grp) == <string>(SELECT grp, math::mean(x) AS a FROM s_mean GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_mean) == <string>(SELECT VALUE a FROM (SELECT math::mean(x) AS a FROM s_mean GROUP ALL)));
CREATE s_mean:1 SET grp = 'c', x = 1;
RETURN (<string>(SELECT grp, a FROM v_mean ORDER BY grp) == <string>(SELECT grp, math::mean(x) AS a FROM s_mean GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_mean) == <string>(SELECT VALUE a FROM (SELECT math::mean(x) AS a FROM s_mean GROUP ALL)));
CREATE s_mean:2 SET grp = 'c';
RETURN (<string>(SELECT grp, a FROM v_mean ORDER BY grp) == <string>(SELECT grp, math::mean(x) AS a FROM s_mean GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_mean) == <string>(SELECT VALUE a FROM (SELECT math::mean(x) AS a FROM s_mean GROUP ALL)));
CREATE s_mean:3 SET grp = 'c', x = 'a';
RETURN (<string>(SELECT grp, a FROM v_mean ORDER BY grp) == <string>(SELECT grp, math::mean(x) AS a FROM s_mean GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_mean) == <string>(SELECT VALUE a FROM (SELECT math::mean(x) AS a FROM s_mean GROUP ALL)));
CREATE s_mean:4 SET grp = 'c', x = [1, 2];
RETURN (<string>(SELECT grp, a FROM v_mean ORDER BY grp) == <string>(SELECT grp, math::mean(x) AS a FROM s_mean GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_mean) == <string>(SELECT VALUE a FROM (SELECT math::mean(x) AS a FROM s_mean GROUP ALL)));
CREATE s_mean:5 SET grp = 'd', x = false;
RETURN (<string>(SELECT grp, a FROM v_mean ORDER BY grp) == <string>(SELECT grp, math::mean(x) AS a FROM s_mean GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_mean) == <string>(SELECT VALUE a FROM (SELECT math::mean(x) AS a FROM s_mean GROUP ALL)));
CREATE s_mean:6 SET grp = 'd', x = NULL;
RETURN (<string>(SELECT grp, a FROM v_mean ORDER BY grp) == <string>(SELECT grp, math::mean(x) AS a FROM s_mean GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_mean) == <string>(SELECT VALUE a FROM (SELECT math::mean(x) AS a FROM s_mean GROUP ALL)));
CREATE s_mean:7 SET grp = 'd', x = 4;
RETURN (<string>(SELECT grp, a FROM v_mean ORDER BY grp) == <string>(SELECT grp, math::mean(x) AS a FROM s_mean GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_mean) == <string>(SELECT VALUE a FROM (SELECT math::mean(x) AS a FROM s_mean GROUP ALL)));
UPDATE s_mean:1 SET grp = 'd';
RETURN (<string>(SELECT grp, a FROM v_mean ORDER BY grp) == <string>(SELECT grp, math::mean(x) AS a FROM s_mean GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_mean) == <string>(SELECT VALUE a FROM (SELECT math::mean(x) AS a FROM s_mean GROUP ALL)));
UPDATE s_mean:3 SET x = 7;
RETURN (<string>(SELECT grp, a FROM v_mean ORDER BY grp) == <string>(SELECT grp, math::mean(x) AS a FROM s_mean GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_mean) == <string>(SELECT VALUE a FROM (SELECT math::mean(x) AS a FROM s_mean GROUP ALL)));
UPDATE s_mean:7 SET x = 'b';
RETURN (<string>(SELECT grp, a FROM v_mean ORDER BY grp) == <string>(SELECT grp, math::mean(x) AS a FROM s_mean GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_mean) == <string>(SELECT VALUE a FROM (SELECT math::mean(x) AS a FROM s_mean GROUP ALL)));
UPDATE s_mean:4 SET x = 10;
RETURN (<string>(SELECT grp, a FROM v_mean ORDER BY grp) == <string>(SELECT grp, math::mean(x) AS a FROM s_mean GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_mean) == <string>(SELECT VALUE a FROM (SELECT math::mean(x) AS a FROM s_mean GROUP ALL)));
UPDATE s_mean:4 SET x = {};
RETURN (<string>(SELECT grp, a FROM v_mean ORDER BY grp) == <string>(SELECT grp, math::mean(x) AS a FROM s_mean GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_mean) == <string>(SELECT VALUE a FROM (SELECT math::mean(x) AS a FROM s_mean GROUP ALL)));
UPDATE s_mean:5 SET grp = 'c';
RETURN (<string>(SELECT grp, a FROM v_mean ORDER BY grp) == <string>(SELECT grp, math::mean(x) AS a FROM s_mean GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_mean) == <string>(SELECT VALUE a FROM (SELECT math::mean(x) AS a FROM s_mean GROUP ALL)));
DELETE s_mean:4;
RETURN (<string>(SELECT grp, a FROM v_mean ORDER BY grp) == <string>(SELECT grp, math::mean(x) AS a FROM s_mean GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_mean) == <string>(SELECT VALUE a FROM (SELECT math::mean(x) AS a FROM s_mean GROUP ALL)));
DELETE s_mean:7;
RETURN (<string>(SELECT grp, a FROM v_mean ORDER BY grp) == <string>(SELECT grp, math::mean(x) AS a FROM s_mean GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_mean) == <string>(SELECT VALUE a FROM (SELECT math::mean(x) AS a FROM s_mean GROUP ALL)));
DELETE s_mean:1;
RETURN (<string>(SELECT grp, a FROM v_mean ORDER BY grp) == <string>(SELECT grp, math::mean(x) AS a FROM s_mean GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_mean) == <string>(SELECT VALUE a FROM (SELECT math::mean(x) AS a FROM s_mean GROUP ALL)));
DELETE s_mean:2;
RETURN (<string>(SELECT grp, a FROM v_mean ORDER BY grp) == <string>(SELECT grp, math::mean(x) AS a FROM s_mean GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_mean) == <string>(SELECT VALUE a FROM (SELECT math::mean(x) AS a FROM s_mean GROUP ALL)));
DELETE s_mean:3;
RETURN (<string>(SELECT grp, a FROM v_mean ORDER BY grp) == <string>(SELECT grp, math::mean(x) AS a FROM s_mean GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_mean) == <string>(SELECT VALUE a FROM (SELECT math::mean(x) AS a FROM s_mean GROUP ALL)));
DELETE s_mean:5;
RETURN (<string>(SELECT grp, a FROM v_mean ORDER BY grp) == <string>(SELECT grp, math::mean(x) AS a FROM s_mean GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_mean) == <string>(SELECT VALUE a FROM (SELECT math::mean(x) AS a FROM s_mean GROUP ALL)));
DELETE s_mean:6;
RETURN (<string>(SELECT grp, a FROM v_mean ORDER BY grp) == <string>(SELECT grp, math::mean(x) AS a FROM s_mean GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_mean) == <string>(SELECT VALUE a FROM (SELECT math::mean(x) AS a FROM s_mean GROUP ALL)));
REMOVE TABLE e_mean;
REMOVE TABLE v_mean;
REMOVE TABLE s_mean;

-- math::stddev(x): view == recompute after each write, GROUP BY and GROUP ALL
DEFINE TABLE s_stddev;
DEFINE TABLE v_stddev AS SELECT grp, math::stddev(x) AS a FROM s_stddev GROUP BY grp;
DEFINE TABLE e_stddev AS SELECT math::stddev(x) AS a FROM s_stddev GROUP ALL;
RETURN (<string>(SELECT grp, a FROM v_stddev ORDER BY grp) == <string>(SELECT grp, math::stddev(x) AS a FROM s_stddev GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_stddev) == <string>(SELECT VALUE a FROM (SELECT math::stddev(x) AS a FROM s_stddev GROUP ALL)));
CREATE s_stddev:1 SET grp = 'c', x = 1;
RETURN (<string>(SELECT grp, a FROM v_stddev ORDER BY grp) == <string>(SELECT grp, math::stddev(x) AS a FROM s_stddev GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_stddev) == <string>(SELECT VALUE a FROM (SELECT math::stddev(x) AS a FROM s_stddev GROUP ALL)));
CREATE s_stddev:2 SET grp = 'c';
RETURN (<string>(SELECT grp, a FROM v_stddev ORDER BY grp) == <string>(SELECT grp, math::stddev(x) AS a FROM s_stddev GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_stddev) == <string>(SELECT VALUE a FROM (SELECT math::stddev(x) AS a FROM s_stddev GROUP ALL)));
CREATE s_stddev:3 SET grp = 'c', x = 'a';
RETURN (<string>(SELECT grp, a FROM v_stddev ORDER BY grp) == <string>(SELECT grp, math::stddev(x) AS a FROM s_stddev GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_stddev) == <string>(SELECT VALUE a FROM (SELECT math::stddev(x) AS a FROM s_stddev GROUP ALL)));
CREATE s_stddev:4 SET grp = 'c', x = [1, 2];
RETURN (<string>(SELECT grp, a FROM v_stddev ORDER BY grp) == <string>(SELECT grp, math::stddev(x) AS a FROM s_stddev GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_stddev) == <string>(SELECT VALUE a FROM (SELECT math::stddev(x) AS a FROM s_stddev GROUP ALL)));
CREATE s_stddev:5 SET grp = 'd', x = false;
RETURN (<string>(SELECT grp, a FROM v_stddev ORDER BY grp) == <string>(SELECT grp, math::stddev(x) AS a FROM s_stddev GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_stddev) == <string>(SELECT VALUE a FROM (SELECT math::stddev(x) AS a FROM s_stddev GROUP ALL)));
CREATE s_stddev:6 SET grp = 'd', x = NULL;
RETURN (<string>(SELECT grp, a FROM v_stddev ORDER BY grp) == <string>(SELECT grp, math::stddev(x) AS a FROM s_stddev GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_stddev) == <string>(SELECT VALUE a FROM (SELECT math::stddev(x) AS a FROM s_stddev GROUP ALL)));
CREATE s_stddev:7 SET grp = 'd', x = 4;
RETURN (<string>(SELECT grp, a FROM v_stddev ORDER BY grp) == <string>(SELECT grp, math::stddev(x) AS a FROM s_stddev GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_stddev) == <string>(SELECT VALUE a FROM (SELECT math::stddev(x) AS a FROM s_stddev GROUP ALL)));
UPDATE s_stddev:1 SET grp = 'd';
RETURN (<string>(SELECT grp, a FROM v_stddev ORDER BY grp) == <string>(SELECT grp, math::stddev(x) AS a FROM s_stddev GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_stddev) == <string>(SELECT VALUE a FROM (SELECT math::stddev(x) AS a FROM s_stddev GROUP ALL)));
UPDATE s_stddev:3 SET x = 7;
RETURN (<string>(SELECT grp, a FROM v_stddev ORDER BY grp) == <string>(SELECT grp, math::stddev(x) AS a FROM s_stddev GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_stddev) == <string>(SELECT VALUE a FROM (SELECT math::stddev(x) AS a FROM s_stddev GROUP ALL)));
UPDATE s_stddev:7 SET x = 'b';
RETURN (<string>(SELECT grp, a FROM v_stddev ORDER BY grp) == <string>(SELECT grp, math::stddev(x) AS a FROM s_stddev GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_stddev) == <string>(SELECT VALUE a FROM (SELECT math::stddev(x) AS a FROM s_stddev GROUP ALL)));
UPDATE s_stddev:4 SET x = 10;
RETURN (<string>(SELECT grp, a FROM v_stddev ORDER BY grp) == <string>(SELECT grp, math::stddev(x) AS a FROM s_stddev GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_stddev) == <string>(SELECT VALUE a FROM (SELECT math::stddev(x) AS a FROM s_stddev GROUP ALL)));
UPDATE s_stddev:4 SET x = {};
RETURN (<string>(SELECT grp, a FROM v_stddev ORDER BY grp) == <string>(SELECT grp, math::stddev(x) AS a FROM s_stddev GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_stddev) == <string>(SELECT VALUE a FROM (SELECT math::stddev(x) AS a FROM s_stddev GROUP ALL)));
UPDATE s_stddev:5 SET grp = 'c';
RETURN (<string>(SELECT grp, a FROM v_stddev ORDER BY grp) == <string>(SELECT grp, math::stddev(x) AS a FROM s_stddev GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_stddev) == <string>(SELECT VALUE a FROM (SELECT math::stddev(x) AS a FROM s_stddev GROUP ALL)));
DELETE s_stddev:4;
RETURN (<string>(SELECT grp, a FROM v_stddev ORDER BY grp) == <string>(SELECT grp, math::stddev(x) AS a FROM s_stddev GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_stddev) == <string>(SELECT VALUE a FROM (SELECT math::stddev(x) AS a FROM s_stddev GROUP ALL)));
DELETE s_stddev:7;
RETURN (<string>(SELECT grp, a FROM v_stddev ORDER BY grp) == <string>(SELECT grp, math::stddev(x) AS a FROM s_stddev GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_stddev) == <string>(SELECT VALUE a FROM (SELECT math::stddev(x) AS a FROM s_stddev GROUP ALL)));
DELETE s_stddev:1;
RETURN (<string>(SELECT grp, a FROM v_stddev ORDER BY grp) == <string>(SELECT grp, math::stddev(x) AS a FROM s_stddev GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_stddev) == <string>(SELECT VALUE a FROM (SELECT math::stddev(x) AS a FROM s_stddev GROUP ALL)));
DELETE s_stddev:2;
RETURN (<string>(SELECT grp, a FROM v_stddev ORDER BY grp) == <string>(SELECT grp, math::stddev(x) AS a FROM s_stddev GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_stddev) == <string>(SELECT VALUE a FROM (SELECT math::stddev(x) AS a FROM s_stddev GROUP ALL)));
DELETE s_stddev:3;
RETURN (<string>(SELECT grp, a FROM v_stddev ORDER BY grp) == <string>(SELECT grp, math::stddev(x) AS a FROM s_stddev GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_stddev) == <string>(SELECT VALUE a FROM (SELECT math::stddev(x) AS a FROM s_stddev GROUP ALL)));
DELETE s_stddev:5;
RETURN (<string>(SELECT grp, a FROM v_stddev ORDER BY grp) == <string>(SELECT grp, math::stddev(x) AS a FROM s_stddev GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_stddev) == <string>(SELECT VALUE a FROM (SELECT math::stddev(x) AS a FROM s_stddev GROUP ALL)));
DELETE s_stddev:6;
RETURN (<string>(SELECT grp, a FROM v_stddev ORDER BY grp) == <string>(SELECT grp, math::stddev(x) AS a FROM s_stddev GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_stddev) == <string>(SELECT VALUE a FROM (SELECT math::stddev(x) AS a FROM s_stddev GROUP ALL)));
REMOVE TABLE e_stddev;
REMOVE TABLE v_stddev;
REMOVE TABLE s_stddev;

-- math::variance(x): view == recompute after each write, GROUP BY and GROUP ALL
DEFINE TABLE s_variance;
DEFINE TABLE v_variance AS SELECT grp, math::variance(x) AS a FROM s_variance GROUP BY grp;
DEFINE TABLE e_variance AS SELECT math::variance(x) AS a FROM s_variance GROUP ALL;
RETURN (<string>(SELECT grp, a FROM v_variance ORDER BY grp) == <string>(SELECT grp, math::variance(x) AS a FROM s_variance GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_variance) == <string>(SELECT VALUE a FROM (SELECT math::variance(x) AS a FROM s_variance GROUP ALL)));
CREATE s_variance:1 SET grp = 'c', x = 1;
RETURN (<string>(SELECT grp, a FROM v_variance ORDER BY grp) == <string>(SELECT grp, math::variance(x) AS a FROM s_variance GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_variance) == <string>(SELECT VALUE a FROM (SELECT math::variance(x) AS a FROM s_variance GROUP ALL)));
CREATE s_variance:2 SET grp = 'c';
RETURN (<string>(SELECT grp, a FROM v_variance ORDER BY grp) == <string>(SELECT grp, math::variance(x) AS a FROM s_variance GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_variance) == <string>(SELECT VALUE a FROM (SELECT math::variance(x) AS a FROM s_variance GROUP ALL)));
CREATE s_variance:3 SET grp = 'c', x = 'a';
RETURN (<string>(SELECT grp, a FROM v_variance ORDER BY grp) == <string>(SELECT grp, math::variance(x) AS a FROM s_variance GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_variance) == <string>(SELECT VALUE a FROM (SELECT math::variance(x) AS a FROM s_variance GROUP ALL)));
CREATE s_variance:4 SET grp = 'c', x = [1, 2];
RETURN (<string>(SELECT grp, a FROM v_variance ORDER BY grp) == <string>(SELECT grp, math::variance(x) AS a FROM s_variance GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_variance) == <string>(SELECT VALUE a FROM (SELECT math::variance(x) AS a FROM s_variance GROUP ALL)));
CREATE s_variance:5 SET grp = 'd', x = false;
RETURN (<string>(SELECT grp, a FROM v_variance ORDER BY grp) == <string>(SELECT grp, math::variance(x) AS a FROM s_variance GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_variance) == <string>(SELECT VALUE a FROM (SELECT math::variance(x) AS a FROM s_variance GROUP ALL)));
CREATE s_variance:6 SET grp = 'd', x = NULL;
RETURN (<string>(SELECT grp, a FROM v_variance ORDER BY grp) == <string>(SELECT grp, math::variance(x) AS a FROM s_variance GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_variance) == <string>(SELECT VALUE a FROM (SELECT math::variance(x) AS a FROM s_variance GROUP ALL)));
CREATE s_variance:7 SET grp = 'd', x = 4;
RETURN (<string>(SELECT grp, a FROM v_variance ORDER BY grp) == <string>(SELECT grp, math::variance(x) AS a FROM s_variance GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_variance) == <string>(SELECT VALUE a FROM (SELECT math::variance(x) AS a FROM s_variance GROUP ALL)));
UPDATE s_variance:1 SET grp = 'd';
RETURN (<string>(SELECT grp, a FROM v_variance ORDER BY grp) == <string>(SELECT grp, math::variance(x) AS a FROM s_variance GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_variance) == <string>(SELECT VALUE a FROM (SELECT math::variance(x) AS a FROM s_variance GROUP ALL)));
UPDATE s_variance:3 SET x = 7;
RETURN (<string>(SELECT grp, a FROM v_variance ORDER BY grp) == <string>(SELECT grp, math::variance(x) AS a FROM s_variance GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_variance) == <string>(SELECT VALUE a FROM (SELECT math::variance(x) AS a FROM s_variance GROUP ALL)));
UPDATE s_variance:7 SET x = 'b';
RETURN (<string>(SELECT grp, a FROM v_variance ORDER BY grp) == <string>(SELECT grp, math::variance(x) AS a FROM s_variance GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_variance) == <string>(SELECT VALUE a FROM (SELECT math::variance(x) AS a FROM s_variance GROUP ALL)));
UPDATE s_variance:4 SET x = 10;
RETURN (<string>(SELECT grp, a FROM v_variance ORDER BY grp) == <string>(SELECT grp, math::variance(x) AS a FROM s_variance GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_variance) == <string>(SELECT VALUE a FROM (SELECT math::variance(x) AS a FROM s_variance GROUP ALL)));
UPDATE s_variance:4 SET x = {};
RETURN (<string>(SELECT grp, a FROM v_variance ORDER BY grp) == <string>(SELECT grp, math::variance(x) AS a FROM s_variance GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_variance) == <string>(SELECT VALUE a FROM (SELECT math::variance(x) AS a FROM s_variance GROUP ALL)));
UPDATE s_variance:5 SET grp = 'c';
RETURN (<string>(SELECT grp, a FROM v_variance ORDER BY grp) == <string>(SELECT grp, math::variance(x) AS a FROM s_variance GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_variance) == <string>(SELECT VALUE a FROM (SELECT math::variance(x) AS a FROM s_variance GROUP ALL)));
DELETE s_variance:4;
RETURN (<string>(SELECT grp, a FROM v_variance ORDER BY grp) == <string>(SELECT grp, math::variance(x) AS a FROM s_variance GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_variance) == <string>(SELECT VALUE a FROM (SELECT math::variance(x) AS a FROM s_variance GROUP ALL)));
DELETE s_variance:7;
RETURN (<string>(SELECT grp, a FROM v_variance ORDER BY grp) == <string>(SELECT grp, math::variance(x) AS a FROM s_variance GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_variance) == <string>(SELECT VALUE a FROM (SELECT math::variance(x) AS a FROM s_variance GROUP ALL)));
DELETE s_variance:1;
RETURN (<string>(SELECT grp, a FROM v_variance ORDER BY grp) == <string>(SELECT grp, math::variance(x) AS a FROM s_variance GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_variance) == <string>(SELECT VALUE a FROM (SELECT math::variance(x) AS a FROM s_variance GROUP ALL)));
DELETE s_variance:2;
RETURN (<string>(SELECT grp, a FROM v_variance ORDER BY grp) == <string>(SELECT grp, math::variance(x) AS a FROM s_variance GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_variance) == <string>(SELECT VALUE a FROM (SELECT math::variance(x) AS a FROM s_variance GROUP ALL)));
DELETE s_variance:3;
RETURN (<string>(SELECT grp, a FROM v_variance ORDER BY grp) == <string>(SELECT grp, math::variance(x) AS a FROM s_variance GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_variance) == <string>(SELECT VALUE a FROM (SELECT math::variance(x) AS a FROM s_variance GROUP ALL)));
DELETE s_variance:5;
RETURN (<string>(SELECT grp, a FROM v_variance ORDER BY grp) == <string>(SELECT grp, math::variance(x) AS a FROM s_variance GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_variance) == <string>(SELECT VALUE a FROM (SELECT math::variance(x) AS a FROM s_variance GROUP ALL)));
DELETE s_variance:6;
RETURN (<string>(SELECT grp, a FROM v_variance ORDER BY grp) == <string>(SELECT grp, math::variance(x) AS a FROM s_variance GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_variance) == <string>(SELECT VALUE a FROM (SELECT math::variance(x) AS a FROM s_variance GROUP ALL)));
REMOVE TABLE e_variance;
REMOVE TABLE v_variance;
REMOVE TABLE s_variance;

-- count(x): view == recompute after each write, GROUP BY and GROUP ALL
DEFINE TABLE s_countx;
DEFINE TABLE v_countx AS SELECT grp, count(x) AS a FROM s_countx GROUP BY grp;
DEFINE TABLE e_countx AS SELECT count(x) AS a FROM s_countx GROUP ALL;
RETURN (<string>(SELECT grp, a FROM v_countx ORDER BY grp) == <string>(SELECT grp, count(x) AS a FROM s_countx GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_countx) == <string>(SELECT VALUE a FROM (SELECT count(x) AS a FROM s_countx GROUP ALL)));
CREATE s_countx:1 SET grp = 'c', x = 1;
RETURN (<string>(SELECT grp, a FROM v_countx ORDER BY grp) == <string>(SELECT grp, count(x) AS a FROM s_countx GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_countx) == <string>(SELECT VALUE a FROM (SELECT count(x) AS a FROM s_countx GROUP ALL)));
CREATE s_countx:2 SET grp = 'c';
RETURN (<string>(SELECT grp, a FROM v_countx ORDER BY grp) == <string>(SELECT grp, count(x) AS a FROM s_countx GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_countx) == <string>(SELECT VALUE a FROM (SELECT count(x) AS a FROM s_countx GROUP ALL)));
CREATE s_countx:3 SET grp = 'c', x = 'a';
RETURN (<string>(SELECT grp, a FROM v_countx ORDER BY grp) == <string>(SELECT grp, count(x) AS a FROM s_countx GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_countx) == <string>(SELECT VALUE a FROM (SELECT count(x) AS a FROM s_countx GROUP ALL)));
CREATE s_countx:4 SET grp = 'c', x = [1, 2];
RETURN (<string>(SELECT grp, a FROM v_countx ORDER BY grp) == <string>(SELECT grp, count(x) AS a FROM s_countx GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_countx) == <string>(SELECT VALUE a FROM (SELECT count(x) AS a FROM s_countx GROUP ALL)));
CREATE s_countx:5 SET grp = 'd', x = false;
RETURN (<string>(SELECT grp, a FROM v_countx ORDER BY grp) == <string>(SELECT grp, count(x) AS a FROM s_countx GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_countx) == <string>(SELECT VALUE a FROM (SELECT count(x) AS a FROM s_countx GROUP ALL)));
CREATE s_countx:6 SET grp = 'd', x = NULL;
RETURN (<string>(SELECT grp, a FROM v_countx ORDER BY grp) == <string>(SELECT grp, count(x) AS a FROM s_countx GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_countx) == <string>(SELECT VALUE a FROM (SELECT count(x) AS a FROM s_countx GROUP ALL)));
CREATE s_countx:7 SET grp = 'd', x = 4;
RETURN (<string>(SELECT grp, a FROM v_countx ORDER BY grp) == <string>(SELECT grp, count(x) AS a FROM s_countx GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_countx) == <string>(SELECT VALUE a FROM (SELECT count(x) AS a FROM s_countx GROUP ALL)));
UPDATE s_countx:1 SET grp = 'd';
RETURN (<string>(SELECT grp, a FROM v_countx ORDER BY grp) == <string>(SELECT grp, count(x) AS a FROM s_countx GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_countx) == <string>(SELECT VALUE a FROM (SELECT count(x) AS a FROM s_countx GROUP ALL)));
UPDATE s_countx:3 SET x = 7;
RETURN (<string>(SELECT grp, a FROM v_countx ORDER BY grp) == <string>(SELECT grp, count(x) AS a FROM s_countx GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_countx) == <string>(SELECT VALUE a FROM (SELECT count(x) AS a FROM s_countx GROUP ALL)));
UPDATE s_countx:7 SET x = 'b';
RETURN (<string>(SELECT grp, a FROM v_countx ORDER BY grp) == <string>(SELECT grp, count(x) AS a FROM s_countx GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_countx) == <string>(SELECT VALUE a FROM (SELECT count(x) AS a FROM s_countx GROUP ALL)));
UPDATE s_countx:4 SET x = 10;
RETURN (<string>(SELECT grp, a FROM v_countx ORDER BY grp) == <string>(SELECT grp, count(x) AS a FROM s_countx GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_countx) == <string>(SELECT VALUE a FROM (SELECT count(x) AS a FROM s_countx GROUP ALL)));
UPDATE s_countx:4 SET x = {};
RETURN (<string>(SELECT grp, a FROM v_countx ORDER BY grp) == <string>(SELECT grp, count(x) AS a FROM s_countx GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_countx) == <string>(SELECT VALUE a FROM (SELECT count(x) AS a FROM s_countx GROUP ALL)));
UPDATE s_countx:5 SET grp = 'c';
RETURN (<string>(SELECT grp, a FROM v_countx ORDER BY grp) == <string>(SELECT grp, count(x) AS a FROM s_countx GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_countx) == <string>(SELECT VALUE a FROM (SELECT count(x) AS a FROM s_countx GROUP ALL)));
DELETE s_countx:4;
RETURN (<string>(SELECT grp, a FROM v_countx ORDER BY grp) == <string>(SELECT grp, count(x) AS a FROM s_countx GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_countx) == <string>(SELECT VALUE a FROM (SELECT count(x) AS a FROM s_countx GROUP ALL)));
DELETE s_countx:7;
RETURN (<string>(SELECT grp, a FROM v_countx ORDER BY grp) == <string>(SELECT grp, count(x) AS a FROM s_countx GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_countx) == <string>(SELECT VALUE a FROM (SELECT count(x) AS a FROM s_countx GROUP ALL)));
DELETE s_countx:1;
RETURN (<string>(SELECT grp, a FROM v_countx ORDER BY grp) == <string>(SELECT grp, count(x) AS a FROM s_countx GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_countx) == <string>(SELECT VALUE a FROM (SELECT count(x) AS a FROM s_countx GROUP ALL)));
DELETE s_countx:2;
RETURN (<string>(SELECT grp, a FROM v_countx ORDER BY grp) == <string>(SELECT grp, count(x) AS a FROM s_countx GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_countx) == <string>(SELECT VALUE a FROM (SELECT count(x) AS a FROM s_countx GROUP ALL)));
DELETE s_countx:3;
RETURN (<string>(SELECT grp, a FROM v_countx ORDER BY grp) == <string>(SELECT grp, count(x) AS a FROM s_countx GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_countx) == <string>(SELECT VALUE a FROM (SELECT count(x) AS a FROM s_countx GROUP ALL)));
DELETE s_countx:5;
RETURN (<string>(SELECT grp, a FROM v_countx ORDER BY grp) == <string>(SELECT grp, count(x) AS a FROM s_countx GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_countx) == <string>(SELECT VALUE a FROM (SELECT count(x) AS a FROM s_countx GROUP ALL)));
DELETE s_countx:6;
RETURN (<string>(SELECT grp, a FROM v_countx ORDER BY grp) == <string>(SELECT grp, count(x) AS a FROM s_countx GROUP BY grp ORDER BY grp)) AND (<string>(SELECT VALUE a FROM e_countx) == <string>(SELECT VALUE a FROM (SELECT count(x) AS a FROM s_countx GROUP ALL)));
REMOVE TABLE e_countx;
REMOVE TABLE v_countx;
REMOVE TABLE s_countx;

