/**
[env]
namespace = true
database = true
imports = ["language/statements/define/privilege/privileged_frame_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:b" }

[test]
reason = """
	A record user's LET of a name a permission reads does not reach the permission,
	wherever the permission reads it: directly, through a function it calls, or in
	a closure. The permission reads the database parameter and the session's own
	values. A field VALUE, ASSERT, DEFAULT, COMPUTED field and event body are held
	to the same frame. An ordinary call, outside any of them, reads the LET.
"""

# LET $allowed = true
[[test.results]]
value = "NONE"

# SELECT through a permission that reads $allowed directly
[[test.results]]
value = "[]"

# SELECT through a permission that calls fn::allowed()
[[test.results]]
value = "[]"

# SELECT through a permission that reads a database parameter
[[test.results]]
value = "[viaparam:1]"

# SELECT through a permission that reads $auth
[[test.results]]
value = "[viaauth:1]"

# SELECT through a permission whose closure reads $allowed
[[test.results]]
value = "[]"

# An ordinary call reads the LET
[[test.results]]
value = "true"

# CREATE with a field VALUE reading $allowed
[[test.results]]
value = "[false]"

# CREATE with a field ASSERT reading $allowed
[[test.results]]
value = "[false]"

# SELECT of a COMPUTED field reading $allowed
[[test.results]]
value = "false"

# CREATE with an event reading $allowed
[[test.results]]
value = "[checked:3]"

# SELECT of the event's records, one for each CREATE above
[[test.results]]
value = "[false, false, false]"

# CREATE with a field DEFAULT reading $allowed
[[test.results]]
value = "[false]"
*/

LET $allowed = true;
SELECT VALUE id FROM direct;
SELECT VALUE id FROM viafn;
SELECT VALUE id FROM viaparam;
SELECT VALUE id FROM viaauth;
SELECT VALUE id FROM viaclosure;
RETURN fn::allowed();
CREATE checked:1 SET v = 1 RETURN VALUE guarded;
CREATE checked:2 SET asserted = false RETURN VALUE asserted;
RETURN (SELECT VALUE computed FROM ONLY checked:1);
CREATE checked:3 SET v = 1 RETURN VALUE id;
SELECT VALUE v FROM evlog;
CREATE checked:4 SET v = 1 RETURN VALUE defaulted;
