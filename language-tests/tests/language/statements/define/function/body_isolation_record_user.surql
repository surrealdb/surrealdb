/**
[env]
namespace = true
database = true
imports = ["language/statements/define/function/body_isolation_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:b" }

[test]
reason = """
	A record user's LET binding of a name a table permission reads, through the
	function it calls, does not widen the permission: the permission's frame
	holds none of the caller's bindings and reads the database parameter of the
	name. An ordinary call, outside a permission, holds none of them either: a
	function body reads its arguments, the row names, the request's parameters
	and the database's parameters, so `fn::lim()` returns the database parameter
	whatever the statement bound.
"""

# LET $allowed = true
[[test.results]]
value = "NONE"

# UPDATE through a permission that asks fn::allowed()
[[test.results]]
value = "[]"

# SELECT through the same permission
[[test.results]]
value = "[]"

# CREATE through the same permission
[[test.results]]
value = "[]"

# LET $limit = 1000
[[test.results]]
value = "NONE"

# CREATE t:1 SET v = fn::lim() RETURN VALUE v
[[test.results]]
value = "[10]"

[[test.results]]
value = "10"

[[test.results]]
value = "10"

[[test.results]]
value = "10"

# FOR $limit IN [5]
[[test.results]]
value = "NONE"

[[test.results]]
value = "[10]"

[[test.results]]
value = "10"

# LET $unbound = 'caller'
[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 10, NONE]"

[[test.results]]
value = "[user:b, 'user']"

[[test.results]]
value = "7"
*/

LET $allowed = true;
UPDATE viafn:1 SET v = 100;
SELECT * FROM viafn;
CREATE viafn:2 SET v = 2;
LET $limit = 1000;
CREATE t:1 SET v = fn::lim() RETURN VALUE v;
RETURN fn::lim();
RETURN fn::lim_block();
RETURN fn::nested();
FOR $limit IN [5] { CREATE t:2 SET v = fn::lim(); };
SELECT VALUE v FROM t:2;
RETURN { LET $limit = 3; fn::lim() };
LET $unbound = 'caller'; RETURN fn::sees(1);
RETURN fn::who();
RETURN fn::shadow(7);
