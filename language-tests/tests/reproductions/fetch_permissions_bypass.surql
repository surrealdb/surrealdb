/**
[env]
imports = ["reproductions/fetch_permissions_bypass_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "user:alice" }

[test]
reason = "Ensure FETCH and field traversal across record links respect table and field permissions"

[[test.results]]
value = "[{ email: 'alice@example.com', id: user:alice }]"

[[test.results]]
value = "[{ id: doc:1, owner: { email: 'alice@example.com', id: user:alice } }]"

[[test.results]]
value = "[{ id: doc:2, owner: NONE }]"

[[test.results]]
value = "[{ owner: NONE }]"

[[test.results]]
value = "[{ owner: NONE }]"

[[test.results]]
value = "[{ owner: { password: NONE } }]"
*/

-- Baseline: only alice visible (table permission WHERE id = $auth), no password (field PERMISSIONS NONE)
SELECT * FROM user;

-- FETCH own linked record: alice visible but password stripped
SELECT * FROM doc:1 FETCH owner;

-- FETCH other user's linked record: bob not accessible (table permission denies)
SELECT * FROM doc:2 FETCH owner;

-- Inline object with FETCH: bob not accessible
SELECT * FROM { owner: user:bob } FETCH owner;

-- Field traversal across record link: bob not accessible
SELECT owner.* FROM { owner: user:bob };

-- Field traversal for restricted field: password not returned even for own record
SELECT owner.password FROM { owner: user:alice };
