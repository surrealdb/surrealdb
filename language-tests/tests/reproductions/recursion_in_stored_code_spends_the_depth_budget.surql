/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	Recursion reached from stored code spends the computation-depth budget as
	the statement nesting, event clauses and function bodies it passes through
	do: a write in a block, a loop, an expression or an IF branch, a function
	body that writes, a table's events, and read-only recursion inside any of
	them each fail with the depth error at the same depth, under the same
	event names, as recursion one level shallower succeeds.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t0:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t4:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t6:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t10:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: u20:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event f: Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event f: Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event f: Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: u24:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event f: Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t26:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t28:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "true"

[[test.results]]
error = "Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]"

*/

DEFINE FUNCTION fn::p($n: int) { $n <= 0 OR fn::p($n - 1) };
DEFINE EVENT e ON t0 WHEN [fn::p(55)] THEN {};
CREATE t0:1;
DEFINE EVENT e ON t1 WHEN [fn::p(56)] THEN {};
CREATE t1:1;
DEFINE EVENT e ON t2 WHEN [fn::p(55)] THEN {};
{ CREATE t2:1 };
DEFINE EVENT e ON t3 WHEN [fn::p(56)] THEN {};
{ CREATE t3:1 };
DEFINE EVENT e ON t4 WHEN [fn::p(53)] THEN {};
IF true { IF true { CREATE t4:1 } };
DEFINE EVENT e ON t5 WHEN [fn::p(54)] THEN {};
IF true { IF true { CREATE t5:1 } };
DEFINE EVENT e ON t6 WHEN [fn::p(53)] THEN {};
{ IF rand::int(1,1) = 1 { CREATE t6:1 } };
DEFINE EVENT e ON t7 WHEN [fn::p(54)] THEN {};
{ IF rand::int(1,1) = 1 { CREATE t7:1 } };
DEFINE EVENT e ON t8 WHEN [fn::p(53)] THEN {};
FOR $i IN [1] { IF true { CREATE t8:1 } };
DEFINE EVENT e ON t9 WHEN [fn::p(54)] THEN {};
FOR $i IN [1] { IF true { CREATE t9:1 } };
DEFINE EVENT e ON t10 WHEN [fn::p(53)] THEN {};
RETURN (IF true { CREATE t10:1 });
DEFINE EVENT e ON t11 WHEN [fn::p(54)] THEN {};
RETURN (IF true { CREATE t11:1 });
DEFINE EVENT e ON t12 WHEN [fn::p(55)] THEN {};
LET $a = (CREATE t12:1);
DEFINE EVENT e ON t13 WHEN [fn::p(56)] THEN {};
LET $a = (CREATE t13:1);
DEFINE EVENT e ON t14 WHEN [fn::p(54)] THEN {};
DEFINE FUNCTION fn::mk14() { CREATE t14:1 }; RETURN fn::mk14();
DEFINE EVENT e ON t15 WHEN [fn::p(55)] THEN {};
DEFINE FUNCTION fn::mk15() { CREATE t15:1 }; RETURN fn::mk15();
DEFINE EVENT e ON t16 WHEN [fn::p(53)] THEN {};
DEFINE FUNCTION fn::mk16() { RETURN { CREATE t16:1 } }; RETURN fn::mk16();
DEFINE EVENT e ON t17 WHEN [fn::p(54)] THEN {};
DEFINE FUNCTION fn::mk17() { RETURN { CREATE t17:1 } }; RETURN fn::mk17();
DEFINE EVENT e ON t18 WHEN [fn::p(53)] THEN {};
DEFINE FUNCTION fn::mk18() { CREATE t18:1 }; DEFINE FUNCTION fn::mk218() { RETURN fn::mk18() }; RETURN fn::mk218();
DEFINE EVENT e ON t19 WHEN [fn::p(54)] THEN {};
DEFINE FUNCTION fn::mk19() { CREATE t19:1 }; DEFINE FUNCTION fn::mk219() { RETURN fn::mk19() }; RETURN fn::mk219();
DEFINE EVENT e ON t20 WHEN [fn::p(52)] THEN {};
DEFINE EVENT f ON u20 THEN { CREATE t20:1 }; CREATE u20:1;
DEFINE EVENT e ON t21 WHEN [fn::p(53)] THEN {};
DEFINE EVENT f ON u21 THEN { CREATE t21:1 }; CREATE u21:1;
DEFINE EVENT e ON t22 WHEN [fn::p(52)] THEN {};
DEFINE FUNCTION fn::mk22() { CREATE t22:1 }; DEFINE EVENT f ON u22 THEN { fn::mk22() }; CREATE u22:1;
DEFINE EVENT e ON t23 WHEN [fn::p(53)] THEN {};
DEFINE FUNCTION fn::mk23() { CREATE t23:1 }; DEFINE EVENT f ON u23 THEN { fn::mk23() }; CREATE u23:1;
DEFINE EVENT e ON t24 WHEN [fn::p(51)] THEN {};
DEFINE EVENT f ON u24 THEN { IF true { CREATE t24:1 } }; CREATE u24:1;
DEFINE EVENT e ON t25 WHEN [fn::p(52)] THEN {};
DEFINE EVENT f ON u25 THEN { IF true { CREATE t25:1 } }; CREATE u25:1;
DEFINE FUNCTION fn::d($n: int) { RETURN IF $n <= 0 { true } ELSE { fn::d($n - 1) } };
DEFINE EVENT e ON t26 THEN { fn::d(27) }; CREATE t26:1;
DEFINE EVENT e ON t27 THEN { fn::d(28) }; CREATE t27:1;
DEFINE EVENT e ON t28 THEN { RETURN fn::d(27) }; CREATE t28:1;
DEFINE EVENT e ON t29 THEN { RETURN fn::d(28) }; CREATE t29:1;
DEFINE FUNCTION fn::w30() { CREATE log30; RETURN fn::d(27) }; RETURN fn::w30();
DEFINE FUNCTION fn::w31() { CREATE log31; RETURN fn::d(28) }; RETURN fn::w31();
DEFINE FUNCTION fn::w32() { CREATE log32; LET $x = fn::d(28); RETURN $x }; RETURN fn::w32();
DEFINE FUNCTION fn::w33() { CREATE log33; LET $x = fn::d(29); RETURN $x }; RETURN fn::w33();
RETURN fn::d(119);
RETURN fn::d(120);
DEFINE EVENT e ON r1 THEN { fn::r1($after.x + 1) }; DEFINE FUNCTION fn::r1($x: int) { CREATE r1 SET x = $x };
CREATE r1:0 SET x = 0;
DEFINE EVENT e ON r2 WHEN $after.x < 10 THEN { fn::r2($after.x + 1) }; DEFINE FUNCTION fn::r2($x: int) { CREATE r2 SET x = $x };
CREATE r2:0 SET x = 0 RETURN NONE;
SELECT VALUE x FROM r2 ORDER BY x;
