/**
[env]
namespace = true
database = true
auth = { level = "owner" }
timeout = 30000

[test]
reason = """
	A chain of sixty tables, each table's event calling a function that
	writes the next table, is lowered with a bounded depth of lowering on the
	stack: every function body and event clause the chain reaches is a unit of
	one arena, and events past the bound are lowered after the outermost. A
	chain that fires fails with the depth error at the depth the budget allows;
	one whose events never fire, or stop after one hop, runs.
"""

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
error = "Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
error = "Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Error while processing event e: Reached excessive computation depth due to functions, subqueries, or computed values"

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
value = "[{ id: q0:1, x: 0 }]"

[[test.results]]
value = "[{ id: q1:1, x: 1 }]"

[[test.results]]
value = "[{ id: q1:1, x: 1 }]"

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
value = "[{ id: w0:1, x: 0 }]"

[[test.results]]
value = "[{ id: w1:1, x: 1 }]"

[[test.results]]
value = "[]"

*/

DEFINE FUNCTION fn::f0() { CREATE c1:1 SET x = 1 }; DEFINE EVENT e ON c0 THEN { fn::f0() };
DEFINE FUNCTION fn::f1() { CREATE c2:1 SET x = 1 }; DEFINE EVENT e ON c1 THEN { fn::f1() };
DEFINE FUNCTION fn::f2() { CREATE c3:1 SET x = 1 }; DEFINE EVENT e ON c2 THEN { fn::f2() };
DEFINE FUNCTION fn::f3() { CREATE c4:1 SET x = 1 }; DEFINE EVENT e ON c3 THEN { fn::f3() };
DEFINE FUNCTION fn::f4() { CREATE c5:1 SET x = 1 }; DEFINE EVENT e ON c4 THEN { fn::f4() };
DEFINE FUNCTION fn::f5() { CREATE c6:1 SET x = 1 }; DEFINE EVENT e ON c5 THEN { fn::f5() };
DEFINE FUNCTION fn::f6() { CREATE c7:1 SET x = 1 }; DEFINE EVENT e ON c6 THEN { fn::f6() };
DEFINE FUNCTION fn::f7() { CREATE c8:1 SET x = 1 }; DEFINE EVENT e ON c7 THEN { fn::f7() };
DEFINE FUNCTION fn::f8() { CREATE c9:1 SET x = 1 }; DEFINE EVENT e ON c8 THEN { fn::f8() };
DEFINE FUNCTION fn::f9() { CREATE c10:1 SET x = 1 }; DEFINE EVENT e ON c9 THEN { fn::f9() };
DEFINE FUNCTION fn::f10() { CREATE c11:1 SET x = 1 }; DEFINE EVENT e ON c10 THEN { fn::f10() };
DEFINE FUNCTION fn::f11() { CREATE c12:1 SET x = 1 }; DEFINE EVENT e ON c11 THEN { fn::f11() };
DEFINE FUNCTION fn::f12() { CREATE c13:1 SET x = 1 }; DEFINE EVENT e ON c12 THEN { fn::f12() };
DEFINE FUNCTION fn::f13() { CREATE c14:1 SET x = 1 }; DEFINE EVENT e ON c13 THEN { fn::f13() };
DEFINE FUNCTION fn::f14() { CREATE c15:1 SET x = 1 }; DEFINE EVENT e ON c14 THEN { fn::f14() };
DEFINE FUNCTION fn::f15() { CREATE c16:1 SET x = 1 }; DEFINE EVENT e ON c15 THEN { fn::f15() };
DEFINE FUNCTION fn::f16() { CREATE c17:1 SET x = 1 }; DEFINE EVENT e ON c16 THEN { fn::f16() };
DEFINE FUNCTION fn::f17() { CREATE c18:1 SET x = 1 }; DEFINE EVENT e ON c17 THEN { fn::f17() };
DEFINE FUNCTION fn::f18() { CREATE c19:1 SET x = 1 }; DEFINE EVENT e ON c18 THEN { fn::f18() };
DEFINE FUNCTION fn::f19() { CREATE c20:1 SET x = 1 }; DEFINE EVENT e ON c19 THEN { fn::f19() };
DEFINE FUNCTION fn::f20() { CREATE c21:1 SET x = 1 }; DEFINE EVENT e ON c20 THEN { fn::f20() };
DEFINE FUNCTION fn::f21() { CREATE c22:1 SET x = 1 }; DEFINE EVENT e ON c21 THEN { fn::f21() };
DEFINE FUNCTION fn::f22() { CREATE c23:1 SET x = 1 }; DEFINE EVENT e ON c22 THEN { fn::f22() };
DEFINE FUNCTION fn::f23() { CREATE c24:1 SET x = 1 }; DEFINE EVENT e ON c23 THEN { fn::f23() };
DEFINE FUNCTION fn::f24() { CREATE c25:1 SET x = 1 }; DEFINE EVENT e ON c24 THEN { fn::f24() };
DEFINE FUNCTION fn::f25() { CREATE c26:1 SET x = 1 }; DEFINE EVENT e ON c25 THEN { fn::f25() };
DEFINE FUNCTION fn::f26() { CREATE c27:1 SET x = 1 }; DEFINE EVENT e ON c26 THEN { fn::f26() };
DEFINE FUNCTION fn::f27() { CREATE c28:1 SET x = 1 }; DEFINE EVENT e ON c27 THEN { fn::f27() };
DEFINE FUNCTION fn::f28() { CREATE c29:1 SET x = 1 }; DEFINE EVENT e ON c28 THEN { fn::f28() };
DEFINE FUNCTION fn::f29() { CREATE c30:1 SET x = 1 }; DEFINE EVENT e ON c29 THEN { fn::f29() };
DEFINE FUNCTION fn::f30() { CREATE c31:1 SET x = 1 }; DEFINE EVENT e ON c30 THEN { fn::f30() };
DEFINE FUNCTION fn::f31() { CREATE c32:1 SET x = 1 }; DEFINE EVENT e ON c31 THEN { fn::f31() };
DEFINE FUNCTION fn::f32() { CREATE c33:1 SET x = 1 }; DEFINE EVENT e ON c32 THEN { fn::f32() };
DEFINE FUNCTION fn::f33() { CREATE c34:1 SET x = 1 }; DEFINE EVENT e ON c33 THEN { fn::f33() };
DEFINE FUNCTION fn::f34() { CREATE c35:1 SET x = 1 }; DEFINE EVENT e ON c34 THEN { fn::f34() };
DEFINE FUNCTION fn::f35() { CREATE c36:1 SET x = 1 }; DEFINE EVENT e ON c35 THEN { fn::f35() };
DEFINE FUNCTION fn::f36() { CREATE c37:1 SET x = 1 }; DEFINE EVENT e ON c36 THEN { fn::f36() };
DEFINE FUNCTION fn::f37() { CREATE c38:1 SET x = 1 }; DEFINE EVENT e ON c37 THEN { fn::f37() };
DEFINE FUNCTION fn::f38() { CREATE c39:1 SET x = 1 }; DEFINE EVENT e ON c38 THEN { fn::f38() };
DEFINE FUNCTION fn::f39() { CREATE c40:1 SET x = 1 }; DEFINE EVENT e ON c39 THEN { fn::f39() };
DEFINE FUNCTION fn::f40() { CREATE c41:1 SET x = 1 }; DEFINE EVENT e ON c40 THEN { fn::f40() };
DEFINE FUNCTION fn::f41() { CREATE c42:1 SET x = 1 }; DEFINE EVENT e ON c41 THEN { fn::f41() };
DEFINE FUNCTION fn::f42() { CREATE c43:1 SET x = 1 }; DEFINE EVENT e ON c42 THEN { fn::f42() };
DEFINE FUNCTION fn::f43() { CREATE c44:1 SET x = 1 }; DEFINE EVENT e ON c43 THEN { fn::f43() };
DEFINE FUNCTION fn::f44() { CREATE c45:1 SET x = 1 }; DEFINE EVENT e ON c44 THEN { fn::f44() };
DEFINE FUNCTION fn::f45() { CREATE c46:1 SET x = 1 }; DEFINE EVENT e ON c45 THEN { fn::f45() };
DEFINE FUNCTION fn::f46() { CREATE c47:1 SET x = 1 }; DEFINE EVENT e ON c46 THEN { fn::f46() };
DEFINE FUNCTION fn::f47() { CREATE c48:1 SET x = 1 }; DEFINE EVENT e ON c47 THEN { fn::f47() };
DEFINE FUNCTION fn::f48() { CREATE c49:1 SET x = 1 }; DEFINE EVENT e ON c48 THEN { fn::f48() };
DEFINE FUNCTION fn::f49() { CREATE c50:1 SET x = 1 }; DEFINE EVENT e ON c49 THEN { fn::f49() };
DEFINE FUNCTION fn::f50() { CREATE c51:1 SET x = 1 }; DEFINE EVENT e ON c50 THEN { fn::f50() };
DEFINE FUNCTION fn::f51() { CREATE c52:1 SET x = 1 }; DEFINE EVENT e ON c51 THEN { fn::f51() };
DEFINE FUNCTION fn::f52() { CREATE c53:1 SET x = 1 }; DEFINE EVENT e ON c52 THEN { fn::f52() };
DEFINE FUNCTION fn::f53() { CREATE c54:1 SET x = 1 }; DEFINE EVENT e ON c53 THEN { fn::f53() };
DEFINE FUNCTION fn::f54() { CREATE c55:1 SET x = 1 }; DEFINE EVENT e ON c54 THEN { fn::f54() };
DEFINE FUNCTION fn::f55() { CREATE c56:1 SET x = 1 }; DEFINE EVENT e ON c55 THEN { fn::f55() };
DEFINE FUNCTION fn::f56() { CREATE c57:1 SET x = 1 }; DEFINE EVENT e ON c56 THEN { fn::f56() };
DEFINE FUNCTION fn::f57() { CREATE c58:1 SET x = 1 }; DEFINE EVENT e ON c57 THEN { fn::f57() };
DEFINE FUNCTION fn::f58() { CREATE c59:1 SET x = 1 }; DEFINE EVENT e ON c58 THEN { fn::f58() };
DEFINE FUNCTION fn::f59() { CREATE c60:1 SET x = 1 }; DEFINE EVENT e ON c59 THEN { fn::f59() };
CREATE c0:1 SET x = 0;
RETURN fn::f0();
DEFINE FUNCTION fn::g0() { CREATE q1:1 SET x = 1 }; DEFINE EVENT e ON q0 WHEN false THEN { fn::g0() };
DEFINE FUNCTION fn::g1() { CREATE q2:1 SET x = 1 }; DEFINE EVENT e ON q1 WHEN false THEN { fn::g1() };
DEFINE FUNCTION fn::g2() { CREATE q3:1 SET x = 1 }; DEFINE EVENT e ON q2 WHEN false THEN { fn::g2() };
DEFINE FUNCTION fn::g3() { CREATE q4:1 SET x = 1 }; DEFINE EVENT e ON q3 WHEN false THEN { fn::g3() };
DEFINE FUNCTION fn::g4() { CREATE q5:1 SET x = 1 }; DEFINE EVENT e ON q4 WHEN false THEN { fn::g4() };
DEFINE FUNCTION fn::g5() { CREATE q6:1 SET x = 1 }; DEFINE EVENT e ON q5 WHEN false THEN { fn::g5() };
DEFINE FUNCTION fn::g6() { CREATE q7:1 SET x = 1 }; DEFINE EVENT e ON q6 WHEN false THEN { fn::g6() };
DEFINE FUNCTION fn::g7() { CREATE q8:1 SET x = 1 }; DEFINE EVENT e ON q7 WHEN false THEN { fn::g7() };
DEFINE FUNCTION fn::g8() { CREATE q9:1 SET x = 1 }; DEFINE EVENT e ON q8 WHEN false THEN { fn::g8() };
DEFINE FUNCTION fn::g9() { CREATE q10:1 SET x = 1 }; DEFINE EVENT e ON q9 WHEN false THEN { fn::g9() };
DEFINE FUNCTION fn::g10() { CREATE q11:1 SET x = 1 }; DEFINE EVENT e ON q10 WHEN false THEN { fn::g10() };
DEFINE FUNCTION fn::g11() { CREATE q12:1 SET x = 1 }; DEFINE EVENT e ON q11 WHEN false THEN { fn::g11() };
DEFINE FUNCTION fn::g12() { CREATE q13:1 SET x = 1 }; DEFINE EVENT e ON q12 WHEN false THEN { fn::g12() };
DEFINE FUNCTION fn::g13() { CREATE q14:1 SET x = 1 }; DEFINE EVENT e ON q13 WHEN false THEN { fn::g13() };
DEFINE FUNCTION fn::g14() { CREATE q15:1 SET x = 1 }; DEFINE EVENT e ON q14 WHEN false THEN { fn::g14() };
DEFINE FUNCTION fn::g15() { CREATE q16:1 SET x = 1 }; DEFINE EVENT e ON q15 WHEN false THEN { fn::g15() };
DEFINE FUNCTION fn::g16() { CREATE q17:1 SET x = 1 }; DEFINE EVENT e ON q16 WHEN false THEN { fn::g16() };
DEFINE FUNCTION fn::g17() { CREATE q18:1 SET x = 1 }; DEFINE EVENT e ON q17 WHEN false THEN { fn::g17() };
DEFINE FUNCTION fn::g18() { CREATE q19:1 SET x = 1 }; DEFINE EVENT e ON q18 WHEN false THEN { fn::g18() };
DEFINE FUNCTION fn::g19() { CREATE q20:1 SET x = 1 }; DEFINE EVENT e ON q19 WHEN false THEN { fn::g19() };
DEFINE FUNCTION fn::g20() { CREATE q21:1 SET x = 1 }; DEFINE EVENT e ON q20 WHEN false THEN { fn::g20() };
DEFINE FUNCTION fn::g21() { CREATE q22:1 SET x = 1 }; DEFINE EVENT e ON q21 WHEN false THEN { fn::g21() };
DEFINE FUNCTION fn::g22() { CREATE q23:1 SET x = 1 }; DEFINE EVENT e ON q22 WHEN false THEN { fn::g22() };
DEFINE FUNCTION fn::g23() { CREATE q24:1 SET x = 1 }; DEFINE EVENT e ON q23 WHEN false THEN { fn::g23() };
DEFINE FUNCTION fn::g24() { CREATE q25:1 SET x = 1 }; DEFINE EVENT e ON q24 WHEN false THEN { fn::g24() };
DEFINE FUNCTION fn::g25() { CREATE q26:1 SET x = 1 }; DEFINE EVENT e ON q25 WHEN false THEN { fn::g25() };
DEFINE FUNCTION fn::g26() { CREATE q27:1 SET x = 1 }; DEFINE EVENT e ON q26 WHEN false THEN { fn::g26() };
DEFINE FUNCTION fn::g27() { CREATE q28:1 SET x = 1 }; DEFINE EVENT e ON q27 WHEN false THEN { fn::g27() };
DEFINE FUNCTION fn::g28() { CREATE q29:1 SET x = 1 }; DEFINE EVENT e ON q28 WHEN false THEN { fn::g28() };
DEFINE FUNCTION fn::g29() { CREATE q30:1 SET x = 1 }; DEFINE EVENT e ON q29 WHEN false THEN { fn::g29() };
DEFINE FUNCTION fn::g30() { CREATE q31:1 SET x = 1 }; DEFINE EVENT e ON q30 WHEN false THEN { fn::g30() };
DEFINE FUNCTION fn::g31() { CREATE q32:1 SET x = 1 }; DEFINE EVENT e ON q31 WHEN false THEN { fn::g31() };
DEFINE FUNCTION fn::g32() { CREATE q33:1 SET x = 1 }; DEFINE EVENT e ON q32 WHEN false THEN { fn::g32() };
DEFINE FUNCTION fn::g33() { CREATE q34:1 SET x = 1 }; DEFINE EVENT e ON q33 WHEN false THEN { fn::g33() };
DEFINE FUNCTION fn::g34() { CREATE q35:1 SET x = 1 }; DEFINE EVENT e ON q34 WHEN false THEN { fn::g34() };
DEFINE FUNCTION fn::g35() { CREATE q36:1 SET x = 1 }; DEFINE EVENT e ON q35 WHEN false THEN { fn::g35() };
DEFINE FUNCTION fn::g36() { CREATE q37:1 SET x = 1 }; DEFINE EVENT e ON q36 WHEN false THEN { fn::g36() };
DEFINE FUNCTION fn::g37() { CREATE q38:1 SET x = 1 }; DEFINE EVENT e ON q37 WHEN false THEN { fn::g37() };
DEFINE FUNCTION fn::g38() { CREATE q39:1 SET x = 1 }; DEFINE EVENT e ON q38 WHEN false THEN { fn::g38() };
DEFINE FUNCTION fn::g39() { CREATE q40:1 SET x = 1 }; DEFINE EVENT e ON q39 WHEN false THEN { fn::g39() };
DEFINE FUNCTION fn::g40() { CREATE q41:1 SET x = 1 }; DEFINE EVENT e ON q40 WHEN false THEN { fn::g40() };
DEFINE FUNCTION fn::g41() { CREATE q42:1 SET x = 1 }; DEFINE EVENT e ON q41 WHEN false THEN { fn::g41() };
DEFINE FUNCTION fn::g42() { CREATE q43:1 SET x = 1 }; DEFINE EVENT e ON q42 WHEN false THEN { fn::g42() };
DEFINE FUNCTION fn::g43() { CREATE q44:1 SET x = 1 }; DEFINE EVENT e ON q43 WHEN false THEN { fn::g43() };
DEFINE FUNCTION fn::g44() { CREATE q45:1 SET x = 1 }; DEFINE EVENT e ON q44 WHEN false THEN { fn::g44() };
DEFINE FUNCTION fn::g45() { CREATE q46:1 SET x = 1 }; DEFINE EVENT e ON q45 WHEN false THEN { fn::g45() };
DEFINE FUNCTION fn::g46() { CREATE q47:1 SET x = 1 }; DEFINE EVENT e ON q46 WHEN false THEN { fn::g46() };
DEFINE FUNCTION fn::g47() { CREATE q48:1 SET x = 1 }; DEFINE EVENT e ON q47 WHEN false THEN { fn::g47() };
DEFINE FUNCTION fn::g48() { CREATE q49:1 SET x = 1 }; DEFINE EVENT e ON q48 WHEN false THEN { fn::g48() };
DEFINE FUNCTION fn::g49() { CREATE q50:1 SET x = 1 }; DEFINE EVENT e ON q49 WHEN false THEN { fn::g49() };
DEFINE FUNCTION fn::g50() { CREATE q51:1 SET x = 1 }; DEFINE EVENT e ON q50 WHEN false THEN { fn::g50() };
DEFINE FUNCTION fn::g51() { CREATE q52:1 SET x = 1 }; DEFINE EVENT e ON q51 WHEN false THEN { fn::g51() };
DEFINE FUNCTION fn::g52() { CREATE q53:1 SET x = 1 }; DEFINE EVENT e ON q52 WHEN false THEN { fn::g52() };
DEFINE FUNCTION fn::g53() { CREATE q54:1 SET x = 1 }; DEFINE EVENT e ON q53 WHEN false THEN { fn::g53() };
DEFINE FUNCTION fn::g54() { CREATE q55:1 SET x = 1 }; DEFINE EVENT e ON q54 WHEN false THEN { fn::g54() };
DEFINE FUNCTION fn::g55() { CREATE q56:1 SET x = 1 }; DEFINE EVENT e ON q55 WHEN false THEN { fn::g55() };
DEFINE FUNCTION fn::g56() { CREATE q57:1 SET x = 1 }; DEFINE EVENT e ON q56 WHEN false THEN { fn::g56() };
DEFINE FUNCTION fn::g57() { CREATE q58:1 SET x = 1 }; DEFINE EVENT e ON q57 WHEN false THEN { fn::g57() };
DEFINE FUNCTION fn::g58() { CREATE q59:1 SET x = 1 }; DEFINE EVENT e ON q58 WHEN false THEN { fn::g58() };
DEFINE FUNCTION fn::g59() { CREATE q60:1 SET x = 1 }; DEFINE EVENT e ON q59 WHEN false THEN { fn::g59() };
CREATE q0:1 SET x = 0;
RETURN fn::g0();
SELECT * FROM q1;
DEFINE FUNCTION fn::h0() { CREATE w1:1 SET x = 1 }; DEFINE EVENT e ON w0 WHEN $after.x < 1 THEN { fn::h0() };
DEFINE FUNCTION fn::h1() { CREATE w2:1 SET x = 1 }; DEFINE EVENT e ON w1 WHEN $after.x < 1 THEN { fn::h1() };
DEFINE FUNCTION fn::h2() { CREATE w3:1 SET x = 1 }; DEFINE EVENT e ON w2 WHEN $after.x < 1 THEN { fn::h2() };
DEFINE FUNCTION fn::h3() { CREATE w4:1 SET x = 1 }; DEFINE EVENT e ON w3 WHEN $after.x < 1 THEN { fn::h3() };
DEFINE FUNCTION fn::h4() { CREATE w5:1 SET x = 1 }; DEFINE EVENT e ON w4 WHEN $after.x < 1 THEN { fn::h4() };
DEFINE FUNCTION fn::h5() { CREATE w6:1 SET x = 1 }; DEFINE EVENT e ON w5 WHEN $after.x < 1 THEN { fn::h5() };
DEFINE FUNCTION fn::h6() { CREATE w7:1 SET x = 1 }; DEFINE EVENT e ON w6 WHEN $after.x < 1 THEN { fn::h6() };
DEFINE FUNCTION fn::h7() { CREATE w8:1 SET x = 1 }; DEFINE EVENT e ON w7 WHEN $after.x < 1 THEN { fn::h7() };
DEFINE FUNCTION fn::h8() { CREATE w9:1 SET x = 1 }; DEFINE EVENT e ON w8 WHEN $after.x < 1 THEN { fn::h8() };
DEFINE FUNCTION fn::h9() { CREATE w10:1 SET x = 1 }; DEFINE EVENT e ON w9 WHEN $after.x < 1 THEN { fn::h9() };
DEFINE FUNCTION fn::h10() { CREATE w11:1 SET x = 1 }; DEFINE EVENT e ON w10 WHEN $after.x < 1 THEN { fn::h10() };
DEFINE FUNCTION fn::h11() { CREATE w12:1 SET x = 1 }; DEFINE EVENT e ON w11 WHEN $after.x < 1 THEN { fn::h11() };
DEFINE FUNCTION fn::h12() { CREATE w13:1 SET x = 1 }; DEFINE EVENT e ON w12 WHEN $after.x < 1 THEN { fn::h12() };
DEFINE FUNCTION fn::h13() { CREATE w14:1 SET x = 1 }; DEFINE EVENT e ON w13 WHEN $after.x < 1 THEN { fn::h13() };
DEFINE FUNCTION fn::h14() { CREATE w15:1 SET x = 1 }; DEFINE EVENT e ON w14 WHEN $after.x < 1 THEN { fn::h14() };
DEFINE FUNCTION fn::h15() { CREATE w16:1 SET x = 1 }; DEFINE EVENT e ON w15 WHEN $after.x < 1 THEN { fn::h15() };
DEFINE FUNCTION fn::h16() { CREATE w17:1 SET x = 1 }; DEFINE EVENT e ON w16 WHEN $after.x < 1 THEN { fn::h16() };
DEFINE FUNCTION fn::h17() { CREATE w18:1 SET x = 1 }; DEFINE EVENT e ON w17 WHEN $after.x < 1 THEN { fn::h17() };
DEFINE FUNCTION fn::h18() { CREATE w19:1 SET x = 1 }; DEFINE EVENT e ON w18 WHEN $after.x < 1 THEN { fn::h18() };
DEFINE FUNCTION fn::h19() { CREATE w20:1 SET x = 1 }; DEFINE EVENT e ON w19 WHEN $after.x < 1 THEN { fn::h19() };
DEFINE FUNCTION fn::h20() { CREATE w21:1 SET x = 1 }; DEFINE EVENT e ON w20 WHEN $after.x < 1 THEN { fn::h20() };
DEFINE FUNCTION fn::h21() { CREATE w22:1 SET x = 1 }; DEFINE EVENT e ON w21 WHEN $after.x < 1 THEN { fn::h21() };
DEFINE FUNCTION fn::h22() { CREATE w23:1 SET x = 1 }; DEFINE EVENT e ON w22 WHEN $after.x < 1 THEN { fn::h22() };
DEFINE FUNCTION fn::h23() { CREATE w24:1 SET x = 1 }; DEFINE EVENT e ON w23 WHEN $after.x < 1 THEN { fn::h23() };
DEFINE FUNCTION fn::h24() { CREATE w25:1 SET x = 1 }; DEFINE EVENT e ON w24 WHEN $after.x < 1 THEN { fn::h24() };
DEFINE FUNCTION fn::h25() { CREATE w26:1 SET x = 1 }; DEFINE EVENT e ON w25 WHEN $after.x < 1 THEN { fn::h25() };
DEFINE FUNCTION fn::h26() { CREATE w27:1 SET x = 1 }; DEFINE EVENT e ON w26 WHEN $after.x < 1 THEN { fn::h26() };
DEFINE FUNCTION fn::h27() { CREATE w28:1 SET x = 1 }; DEFINE EVENT e ON w27 WHEN $after.x < 1 THEN { fn::h27() };
DEFINE FUNCTION fn::h28() { CREATE w29:1 SET x = 1 }; DEFINE EVENT e ON w28 WHEN $after.x < 1 THEN { fn::h28() };
DEFINE FUNCTION fn::h29() { CREATE w30:1 SET x = 1 }; DEFINE EVENT e ON w29 WHEN $after.x < 1 THEN { fn::h29() };
DEFINE FUNCTION fn::h30() { CREATE w31:1 SET x = 1 }; DEFINE EVENT e ON w30 WHEN $after.x < 1 THEN { fn::h30() };
DEFINE FUNCTION fn::h31() { CREATE w32:1 SET x = 1 }; DEFINE EVENT e ON w31 WHEN $after.x < 1 THEN { fn::h31() };
DEFINE FUNCTION fn::h32() { CREATE w33:1 SET x = 1 }; DEFINE EVENT e ON w32 WHEN $after.x < 1 THEN { fn::h32() };
DEFINE FUNCTION fn::h33() { CREATE w34:1 SET x = 1 }; DEFINE EVENT e ON w33 WHEN $after.x < 1 THEN { fn::h33() };
DEFINE FUNCTION fn::h34() { CREATE w35:1 SET x = 1 }; DEFINE EVENT e ON w34 WHEN $after.x < 1 THEN { fn::h34() };
DEFINE FUNCTION fn::h35() { CREATE w36:1 SET x = 1 }; DEFINE EVENT e ON w35 WHEN $after.x < 1 THEN { fn::h35() };
DEFINE FUNCTION fn::h36() { CREATE w37:1 SET x = 1 }; DEFINE EVENT e ON w36 WHEN $after.x < 1 THEN { fn::h36() };
DEFINE FUNCTION fn::h37() { CREATE w38:1 SET x = 1 }; DEFINE EVENT e ON w37 WHEN $after.x < 1 THEN { fn::h37() };
DEFINE FUNCTION fn::h38() { CREATE w39:1 SET x = 1 }; DEFINE EVENT e ON w38 WHEN $after.x < 1 THEN { fn::h38() };
DEFINE FUNCTION fn::h39() { CREATE w40:1 SET x = 1 }; DEFINE EVENT e ON w39 WHEN $after.x < 1 THEN { fn::h39() };
DEFINE FUNCTION fn::h40() { CREATE w41:1 SET x = 1 }; DEFINE EVENT e ON w40 WHEN $after.x < 1 THEN { fn::h40() };
DEFINE FUNCTION fn::h41() { CREATE w42:1 SET x = 1 }; DEFINE EVENT e ON w41 WHEN $after.x < 1 THEN { fn::h41() };
DEFINE FUNCTION fn::h42() { CREATE w43:1 SET x = 1 }; DEFINE EVENT e ON w42 WHEN $after.x < 1 THEN { fn::h42() };
DEFINE FUNCTION fn::h43() { CREATE w44:1 SET x = 1 }; DEFINE EVENT e ON w43 WHEN $after.x < 1 THEN { fn::h43() };
DEFINE FUNCTION fn::h44() { CREATE w45:1 SET x = 1 }; DEFINE EVENT e ON w44 WHEN $after.x < 1 THEN { fn::h44() };
DEFINE FUNCTION fn::h45() { CREATE w46:1 SET x = 1 }; DEFINE EVENT e ON w45 WHEN $after.x < 1 THEN { fn::h45() };
DEFINE FUNCTION fn::h46() { CREATE w47:1 SET x = 1 }; DEFINE EVENT e ON w46 WHEN $after.x < 1 THEN { fn::h46() };
DEFINE FUNCTION fn::h47() { CREATE w48:1 SET x = 1 }; DEFINE EVENT e ON w47 WHEN $after.x < 1 THEN { fn::h47() };
DEFINE FUNCTION fn::h48() { CREATE w49:1 SET x = 1 }; DEFINE EVENT e ON w48 WHEN $after.x < 1 THEN { fn::h48() };
DEFINE FUNCTION fn::h49() { CREATE w50:1 SET x = 1 }; DEFINE EVENT e ON w49 WHEN $after.x < 1 THEN { fn::h49() };
DEFINE FUNCTION fn::h50() { CREATE w51:1 SET x = 1 }; DEFINE EVENT e ON w50 WHEN $after.x < 1 THEN { fn::h50() };
DEFINE FUNCTION fn::h51() { CREATE w52:1 SET x = 1 }; DEFINE EVENT e ON w51 WHEN $after.x < 1 THEN { fn::h51() };
DEFINE FUNCTION fn::h52() { CREATE w53:1 SET x = 1 }; DEFINE EVENT e ON w52 WHEN $after.x < 1 THEN { fn::h52() };
DEFINE FUNCTION fn::h53() { CREATE w54:1 SET x = 1 }; DEFINE EVENT e ON w53 WHEN $after.x < 1 THEN { fn::h53() };
DEFINE FUNCTION fn::h54() { CREATE w55:1 SET x = 1 }; DEFINE EVENT e ON w54 WHEN $after.x < 1 THEN { fn::h54() };
DEFINE FUNCTION fn::h55() { CREATE w56:1 SET x = 1 }; DEFINE EVENT e ON w55 WHEN $after.x < 1 THEN { fn::h55() };
DEFINE FUNCTION fn::h56() { CREATE w57:1 SET x = 1 }; DEFINE EVENT e ON w56 WHEN $after.x < 1 THEN { fn::h56() };
DEFINE FUNCTION fn::h57() { CREATE w58:1 SET x = 1 }; DEFINE EVENT e ON w57 WHEN $after.x < 1 THEN { fn::h57() };
DEFINE FUNCTION fn::h58() { CREATE w59:1 SET x = 1 }; DEFINE EVENT e ON w58 WHEN $after.x < 1 THEN { fn::h58() };
DEFINE FUNCTION fn::h59() { CREATE w60:1 SET x = 1 }; DEFINE EVENT e ON w59 WHEN $after.x < 1 THEN { fn::h59() };
CREATE w0:1 SET x = 0;
SELECT * FROM w1;
SELECT * FROM w2;
