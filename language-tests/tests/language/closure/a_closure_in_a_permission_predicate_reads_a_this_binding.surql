/**
[env]
namespace = true
database = true
imports = ["language/closure/a_closure_in_a_permission_predicate_reads_a_this_binding_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:test" }

[test]
reason = "A record user's `PERMISSIONS FOR select WHERE` predicate that holds a closure reads, in the closure, the `$this` a block `LET` binds where the literal is written, as the closure-free twin reads it: both tables admit both rows. Runs as a record user."

[[test.results]]
value = "[pt_this:1, pt_this:2]"

[[test.results]]
value = "[pt_this_cf:1, pt_this_cf:2]"
*/
SELECT VALUE id FROM pt_this ORDER BY id;
SELECT VALUE id FROM pt_this_cf ORDER BY id;
