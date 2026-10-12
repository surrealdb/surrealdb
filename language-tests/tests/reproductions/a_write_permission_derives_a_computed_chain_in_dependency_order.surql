/**
[env]
imports = ["reproductions/a_write_permission_derives_a_computed_chain_in_dependency_order_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "A table-level write `PERMISSIONS` predicate reads a COMPUTED field at the end of a three-field chain. The write gate derives the fields the predicate names and what they read, or every computed field when the predicate's dependencies cannot be resolved statically; either way it has to derive each field after the fields it reads, the order the streaming read path uses. In name order the predicate field is derived from NONE and admits a record user's write to a closed record."

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ n: 1 }]"

[[test.results]]
value = "[{ allowed: true, id: doc:fresh }]"

[[test.results]]
value = "[{ allowed: false, blocked: true, closed: true, id: doc:closed, n: NONE }, { allowed: true, blocked: false, closed: false, id: doc:fresh, n: NONE }, { allowed: true, blocked: false, closed: false, id: doc:open, n: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ n: 1 }]"

[[test.results]]
value = "[{ allowed: false, blocked: true, closed: true, id: memo:closed, n: NONE }, { allowed: true, blocked: false, closed: false, id: memo:open, n: 1 }]"







*/

-- `closed` is true, so `blocked` is true and `allowed` is false: the update is
-- rejected, and a rejected record is filtered out of the result.
UPDATE doc:closed SET n = 1;
-- The delete gate reads the same chain. `RETURN BEFORE` would return the record
-- had the delete been admitted.
DELETE doc:closed RETURN BEFORE;
-- The create gate is judged against the record about to be created.
CREATE doc:new SET status = 'closed';
-- An open record is still writable, and an open record can still be created.
UPDATE doc:open SET n = 1 RETURN n;
CREATE doc:fresh SET status = 'open' RETURN id, allowed;
-- Nothing was created, modified or removed on a closed record, and the read
-- path derives the same values the gate judged.
SELECT id, closed, blocked, allowed, n FROM doc ORDER BY id;

-- The same writes through the predicate that derives every computed field.
UPDATE memo:closed SET n = 1;
DELETE memo:closed RETURN BEFORE;
CREATE memo:new SET status = 'closed';
UPDATE memo:open SET n = 1 RETURN n;
SELECT id, closed, blocked, allowed, n FROM memo ORDER BY id;
