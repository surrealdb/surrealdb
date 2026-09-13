/**
[env]
imports = ["reproductions/closure_call_guard_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = "a closure invoked at runtime reads the row through a body the predicate does not hold, so the fields it may reach keep their guard columns"

[[test.results]]
value = "[{ id: t:1, public: 'a', tags: ['x'] }, { id: t:2, public: 'b', tags: ['y'] }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"
*/

-- `secret` is denied outright, so the record user reads neither value.
SELECT * FROM t;

-- A predicate that names the field is judged against the guarded column and
-- admits nothing. The invocation forms below must answer the same: the body
-- is nowhere in the predicate, so nothing about it says which fields it
-- reaches, and the row it runs against has to be the guarded one.
SELECT VALUE public FROM t WHERE secret = 'yes';

LET $pred = || secret = 'yes';
SELECT VALUE public FROM t WHERE $pred();

LET $o = { check: || secret = 'yes' };
SELECT VALUE public FROM t WHERE $o.check();

LET $fns = [|| secret = 'yes'];
SELECT VALUE public FROM t WHERE $fns[0]();
