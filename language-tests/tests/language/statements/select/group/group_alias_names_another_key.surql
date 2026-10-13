/**
[test]
reason = "A projected field is the group key its alias names in GROUP BY, even when the field's own path is the name of another key. In `SELECT g1 AS g0, g2 AS g1 ... GROUP BY g0, g1` the path g1 that the first field projects is also the second key's name, yet the first field is key 0 and the second is key 1. The field's path decides only when its alias names no key."

[[test.results]]
value = "[{ g1: 'a', g2: true, id: gk:1, n: 1 }]"

[[test.results]]
value = "[{ g1: 'a', g2: true, id: gk:2, n: 2 }]"

[[test.results]]
value = "[{ g1: 'x', g2: false, id: gk:3, n: 4 }]"

[[test.results]]
value = "[{ c: 2, g0: 'a', g1: true }, { c: 1, g0: 'x', g1: false }]"

[[test.results]]
value = "[{ c: 2, g1: true, g2: 'a' }, { c: 1, g1: false, g2: 'x' }]"

[[test.results]]
value = "[{ c: 2, g0: 'a', g1: true }, { c: 1, g0: 'x', g1: false }]"

[[test.results]]
value = "[{ c: 2, g0: 'a', g1: true, s: 3 }, { c: 1, g0: 'x', g1: false, s: 4 }]"

[[test.results]]
value = "[{ c: 2, g1: 'a', g2: true }, { c: 1, g1: 'x', g2: false }]"

[[test.results]]
value = "[{ c: 2, g2: 'a' }, { c: 1, g2: 'x' }]"

[[test.results]]
value = "[{ c: 2, first: 'a', g2: true }, { c: 1, first: 'x', g2: false }]"
*/

CREATE gk:1 SET g1 = 'a', g2 = true, n = 1;
CREATE gk:2 SET g1 = 'a', g2 = true, n = 2;
CREATE gk:3 SET g1 = 'x', g2 = false, n = 4;

-- Each alias is the name of another field's path.
SELECT g1 AS g0, g2 AS g1, count() AS c FROM gk GROUP BY g0, g1 ORDER BY g0;

-- Aliases that swap two source names.
SELECT g1 AS g2, g2 AS g1, count() AS c FROM gk GROUP BY g2, g1 ORDER BY g2;

-- The keys written in the other order than they are projected.
SELECT g1 AS g0, g2 AS g1, count() AS c FROM gk GROUP BY g1, g0 ORDER BY g0;

-- An aggregate beside the aliased keys.
SELECT g1 AS g0, g2 AS g1, count() AS c, math::sum(n) AS s FROM gk GROUP BY g0, g1 ORDER BY g0;

-- An alias that names its own path, and one that names no source field.
SELECT g1 AS g1, g2 AS g2, count() AS c FROM gk GROUP BY g1, g2 ORDER BY g1;
SELECT g1 AS g2, count() AS c FROM gk GROUP BY g2 ORDER BY g2;

-- An alias that names no key leaves the path to decide.
SELECT g1 AS first, g2, count() AS c FROM gk GROUP BY g1, g2 ORDER BY first;
