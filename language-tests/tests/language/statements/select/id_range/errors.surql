/**
[env]
namespace = true
database = true

[test]
reason = """
	A predicate judges every row of a table scan, so a conjunct that raises on a row below an id
	bound raises whether it is written before or after the bound. A scan narrowed to the bound would
	never meet that row, so a conjunct that may raise keeps the scan whole and the statement raises
	the error the whole scan raises; a conjunct that cannot raise leaves the bound to narrow it.
"""

[[test.results]]
value = "[{ id: t:1, n: 1, s: 'x' }]"

[[test.results]]
value = "[{ id: t:2, n: 2, s: '2' }]"

[[test.results]]
value = "[{ id: t:3, n: 3, s: '3' }]"

[[test.results]]
value = "[{ id: t:4, n: 4, s: '4' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[t:3, t:4]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[t:2, t:3, t:4]"

[[test.results]]
value = "[t:3]"

[[test.results]]
value = "[t:3, t:4]"

[[test.results]]
error = "An error occurred: boom"

[[test.results]]
value = "[t:2, t:3, t:4]"

[[test.results]]
value = "[t:3, t:4]"

[[test.results]]
value = "[t:3, t:4]"

[[test.results]]
value = "[t:3, t:4]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[t:3, t:4]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[t:3, t:4]"

[[test.results]]
value = "[t:1, t:2, t:3, t:4]"
*/

CREATE t:1 SET s = 'x', n = 1;
CREATE t:2 SET s = '2', n = 2;
CREATE t:3 SET s = '3', n = 3;
CREATE t:4 SET s = '4', n = 4;
DEFINE FUNCTION fn::raise_on($n: int) { IF $n = 1 { THROW 'boom'; }; RETURN true; };
-- The cast rejects the row below the bound.
SELECT VALUE id FROM t WHERE id > t:2 AND <int> s > 0;
SELECT VALUE id FROM t WHERE <int> s > 0 AND id > t:2;
SELECT VALUE id FROM t WHERE id > t:1 AND <int> s > 0;
SELECT VALUE id FROM t WHERE id > t:2 AND <int> s > 0 AND id < t:4;
-- A function that raises on the row below the bound.
SELECT VALUE id FROM t WHERE id > t:2 AND fn::raise_on(n);
SELECT VALUE id FROM t WHERE fn::raise_on(n) AND id > t:2;
SELECT VALUE id FROM t WHERE id > t:1 AND fn::raise_on(n);
-- A conjunct that cannot raise leaves the bound to narrow the scan.
SELECT VALUE id FROM t WHERE id > t:2 AND n > 0;
SELECT VALUE id FROM t WHERE n > 0 AND id > t:2 AND s != 'y';
-- A raising conjunct behind an OR that a bound row satisfies.
SELECT VALUE id FROM t WHERE id > t:2 AND (n > 0 OR <int> s > 0);
-- A bound that is a parameter, beside a conjunct that raises.
LET $c = t:2;
SELECT VALUE id FROM t WHERE id > $c AND <int> s > 0;
SELECT VALUE id FROM t WHERE <int> s > 0 AND id > $c;
-- A parameter the statement does not define, in the bound or beside it.
SELECT VALUE id FROM t WHERE id > t:2 AND n > $missing;
SELECT VALUE id FROM t WHERE id > $missing AND n > 0;
