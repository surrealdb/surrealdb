/**
[env]
namespace = true
database = true

[test]
reason = "Legacy `type::record(<value>, 'table')` form must still validate the value's table when `<value>` is a record id (Codex finding 97589bcc). Without the guard, a SIGNIN clause like SELECT * FROM type::record($id, 'user') would silently accept attacker-controlled record ids from a different table."

[[test.results]]
value = "user:alice"

[[test.results]]
match = "$error = /expected a record in table 'user'/"

[[test.results]]
value = "user:bob"
*/

-- Legitimate use: record already in 'user' table -> passes.
type::record(user:alice, "user");
-- Attack: caller-controlled value points at a different table -> rejected.
type::record(other:evil, "user");
-- New-style call (table, key) keeps working: arg1 is a plain string.
type::record("user", "bob");
