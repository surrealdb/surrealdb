/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A WHERE id = <record id> whose key is an array or an object reads that one record, as a number or string key does, whether its elements are literals or parameters"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[[1], []]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"
*/
CREATE metric:['a', 2026] SET v = 1 RETURN NONE;
CREATE metric:['b', 2025] SET v = 2 RETURN NONE;
CREATE metric:{ k: 'a', y: 2026 } SET v = 3 RETURN NONE;
SELECT VALUE v FROM metric WHERE id = metric:['a', 2026];
SELECT VALUE v FROM metric WHERE id = metric:{ k: 'a', y: 2026 } AND v > 0;
SELECT VALUE v FROM metric WHERE v > 0 AND id = r"metric:['b', 2025]";
SELECT VALUE v FROM metric WHERE id = metric:['a', 2027];
{ LET $y = 2026; RETURN SELECT VALUE v FROM metric WHERE id = metric:['a', $y]; };
{ LET $k = 'a'; RETURN SELECT VALUE v FROM metric WHERE id = metric:{ k: $k, y: 2026 }; };
SELECT VALUE (SELECT VALUE v FROM metric WHERE id = metric:[$parent.k, 2026]) FROM [{ k: 'a' }, { k: 'b' }];
SELECT VALUE v FROM metric WHERE id = metric:[v, 2026];
LET $plan = (EXPLAIN SELECT VALUE v FROM metric WHERE id = metric:['a', 2026]);
string::contains(<string> $plan, 'RecordIdScan') AND !string::contains(<string> $plan, 'TableScan');
