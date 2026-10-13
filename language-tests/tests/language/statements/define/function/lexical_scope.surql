/**
[env]
namespace = true
database = true

[test]
reason = """
	A function body is lexically scoped. It reads its arguments, the names it
	binds itself, the row names and the database's parameters, and never a
	binding of its caller, whether that is a LET of the request, a LET inside a
	block, a FOR variable, a closure argument, or the LET or argument of a
	calling function, at any depth of calls. A name the body does not bind is
	the database's parameter of that name, or NONE when the database defines
	none. The caller itself reads its own binding, which shadows the
	database's parameter for it alone. A closure written in a body captures
	where the body is written, so it reads what the body reads.
"""

# DEFINE PARAM $p VALUE 3
[[test.results]]
value = "NONE"

# DEFINE FUNCTION fn::read_p()
[[test.results]]
value = "NONE"

# DEFINE FUNCTION fn::read_q()
[[test.results]]
value = "NONE"

# DEFINE FUNCTION fn::read_arg()
[[test.results]]
value = "NONE"

# DEFINE FUNCTION fn::calls_inner()
[[test.results]]
value = "NONE"

# DEFINE FUNCTION fn::own_let()
[[test.results]]
value = "NONE"

# DEFINE FUNCTION fn::with_closure()
[[test.results]]
value = "NONE"

# DEFINE FUNCTION fn::row()
[[test.results]]
value = "NONE"

# DEFINE TABLE t
[[test.results]]
value = "NONE"

# DEFINE TABLE r
[[test.results]]
value = "NONE"

# CREATE r:1 SET v = 7
[[test.results]]
value = "[{ id: r:1, v: 7 }]"

# Without a binding the caller and the body read the database's parameter
[[test.results]]
value = "[3, 3]"

# LET $p = 5, the request's LET
[[test.results]]
value = "NONE"

# The caller reads its LET, the body the database's parameter
[[test.results]]
value = "[5, 3]"

# A block's LET
[[test.results]]
value = "[7, 3]"

# FOR $p IN [9] { CREATE t:f SET v = fn::read_p() }
[[test.results]]
value = "NONE"

[[test.results]]
value = "[3]"

# A closure argument named like the parameter
[[test.results]]
value = "[3]"

# A closure captures the caller's LET for its own body, and the function it
# calls still reads the database's parameter
[[test.results]]
value = "[[5, 3]]"

# LET $q = 'caller', where no parameter of that name exists
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

# fn::read_arg(1) reads its argument and the database's parameter
[[test.results]]
value = "[1, 3]"

# A function called from a function reads no binding of the first one: not
# its argument $p, and not the request's LET $q
[[test.results]]
value = "[1, 3, NONE]"

# A function's own LET is read by it and by nothing it calls
[[test.results]]
value = "['own', 3]"

# A closure written in a body captures what the body reads
[[test.results]]
value = "[[1, 3]]"

# The row names reach a body called over a row
[[test.results]]
value = "[7]"
*/

DEFINE PARAM $p VALUE 3 PERMISSIONS FULL;
DEFINE FUNCTION fn::read_p() { RETURN $p } PERMISSIONS FULL;
DEFINE FUNCTION fn::read_q() { RETURN $q } PERMISSIONS FULL;
DEFINE FUNCTION fn::read_arg($a: int) { RETURN [$a, $p] } PERMISSIONS FULL;
DEFINE FUNCTION fn::calls_inner($p: int) { RETURN [$p, fn::read_p(), fn::read_q()] } PERMISSIONS FULL;
DEFINE FUNCTION fn::own_let() { LET $p = 'own'; RETURN [$p, fn::read_p()] } PERMISSIONS FULL;
DEFINE FUNCTION fn::with_closure() { RETURN [1].map(|$x| [$x, $p]) } PERMISSIONS FULL;
DEFINE FUNCTION fn::row() { RETURN $this.v } PERMISSIONS FULL;
DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE r SCHEMALESS PERMISSIONS FULL;
CREATE r:1 SET v = 7;
RETURN [$p, fn::read_p()];
LET $p = 5;
RETURN [$p, fn::read_p()];
RETURN { LET $p = 7; [$p, fn::read_p()] };
FOR $p IN [9] { CREATE t:f SET v = fn::read_p(); };
SELECT VALUE v FROM t:f;
RETURN [1].map(|$p| fn::read_p());
RETURN [1].map(|$x| [$p, fn::read_p()]);
LET $q = 'caller';
RETURN fn::read_q();
RETURN fn::read_arg(1);
RETURN fn::calls_inner(1);
RETURN fn::own_let();
RETURN fn::with_closure();
SELECT VALUE fn::row() FROM r;
