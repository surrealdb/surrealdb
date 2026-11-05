/**
[env]
namespace = true
database = true
auth = { level = "owner" }
timeout = 10000

[test]
reason = """
A function body is compiled once and kept for later queries, so a time::now()
in a condition of its body reads the clock when the call runs: a record whose
expiry has passed by a later call is no longer returned.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[share:far, share:soon]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[share:far]"

[[test.results]]
value = "[share:far]"

*/
DEFINE FUNCTION fn::live() { RETURN SELECT VALUE id FROM share WHERE expires > time::now() ORDER BY id; };
CREATE share:soon SET expires = time::now() + 1500ms RETURN NONE;
CREATE share:far SET expires = time::now() + 1h RETURN NONE;
RETURN fn::live();
SLEEP 2s;
RETURN fn::live();
RETURN (SELECT VALUE id FROM share WHERE expires > time::now() ORDER BY id);
