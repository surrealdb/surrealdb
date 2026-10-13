/**
[env]
imports = ["language/closure/capture_values_record_user_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = """
A database parameter no binding shadows is read when a closure's body runs, with
the permissions of the invoker, and a name the body binds itself is not the
parameter. A record user who may not view the parameter creates a closure that
names it, and invoking the closure raises the permission error, not creating it.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
error = "You don't have permission to view the $r parameter"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

[[test.results]]
value = "[2]"
*/

LET $f = |$e| $r;
RETURN 1;
RETURN $f(1);
LET $h = |$e| { LET $r = 2; $r };
RETURN $h(1);
CREATE w:1 RETURN VALUE f;
