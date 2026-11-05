/**
[env]
namespace = true
database = true

[test]
reason = """
	A function body reads its arguments, the row names and the ordinary chain:
	the bind parameters, the session values and the database's parameters. It
	never reads a binding of its caller, whether a LET, a block-local LET or a
	FOR variable of the calling statement. A name the body's own scope does not
	bind is the database's parameter of that name, or NONE when the database
	defines none.
"""

# DEFINE PARAM $limit VALUE 10
[[test.results]]
value = "NONE"

# DEFINE FUNCTION fn::lim()
[[test.results]]
value = "NONE"

# DEFINE FUNCTION fn::get_x()
[[test.results]]
value = "NONE"

# DEFINE FUNCTION fn::get_y()
[[test.results]]
value = "NONE"

# LET $limit = 42
[[test.results]]
value = "NONE"

# RETURN fn::lim(): the caller's LET is not seen
[[test.results]]
value = "10"

# RETURN { LET $limit = 3; fn::lim() }
[[test.results]]
value = "10"

# FOR $limit IN [5] { CREATE caller:1 SET v = fn::lim() }
[[test.results]]
value = "NONE"

[[test.results]]
value = "[10]"

# LET $x = 'bound'
[[test.results]]
value = "NONE"

# RETURN fn::get_x(), which reads a name only the caller binds
[[test.results]]
value = "NONE"

# RETURN fn::get_y(), which binds no $y anywhere
[[test.results]]
value = "NONE"
*/

DEFINE PARAM $limit VALUE 10 PERMISSIONS FULL;
DEFINE FUNCTION fn::lim() { RETURN $limit } PERMISSIONS FULL;
DEFINE FUNCTION fn::get_x() { RETURN $x } PERMISSIONS FULL;
DEFINE FUNCTION fn::get_y() { RETURN $y } PERMISSIONS FULL;
LET $limit = 42;
RETURN fn::lim();
RETURN { LET $limit = 3; fn::lim() };
FOR $limit IN [5] { CREATE caller:1 SET v = fn::lim(); };
SELECT VALUE v FROM caller:1;
LET $x = 'bound';
RETURN fn::get_x();
RETURN fn::get_y();
