/**
[env.capabilities]
deny-net = true

[test]
reason = "A fold must not discard an operand whose denial comes from a capability other than the function list"

[[test.results]]
value = "[{ a: 5, id: t:1 }]"

[[test.results]]
error = "Remote HTTP request functions are not enabled"

[[test.results]]
error = "Remote HTTP request functions are not enabled"

*/

CREATE t:1 SET a = 5;

-- Every function name is permitted here; the denial comes from the network
-- capability that `http::get` enforces on top of its name. A rule keyed on
-- the function list alone would read this operand as harmless and discard it,
-- so what a fold may discard cannot be decided from function permissions.
SELECT * FROM t WHERE http::get('https://example.com') IN [];
SELECT * FROM t WHERE http::get('https://example.com') > 0;
