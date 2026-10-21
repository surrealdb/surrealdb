/**
[env]
namespace = true
database = true

[test]
reason = "$auth, $access, $token and $session are protected variables. LET, UNSET and the set RPCs refuse them, but the constructs that bind a user-chosen name - a FOR variable, a closure parameter and a user-defined function argument - once bound them unchecked. A permission predicate reads the session's value of a protected name from the frame it runs in, so a binding of one would hand the predicate a caller-chosen identity, letting a low-privilege session forge $auth and bypass table permissions (GHSA-ffvm-mqh3-7986). DEFINE FUNCTION and ALTER FUNCTION both refuse a protected argument name. A FOR variable and a closure parameter refuse one as LET does: the refusal is the whole statement's where it is planned eagerly, and is raised only if the binding is reached where the closure is bound after a catalog change in the same block, so a statement that already ran still stands. A non-protected name still binds, and an unshadowed read resolves against the session."

[[test.results]]
error = "'session' is a protected variable and cannot be set"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
error = "'session' is a protected variable and cannot be set"

[[test.results]]
error = "'token' is a protected variable and cannot be set"

[[test.results]]
value = "NONE"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

[[test.results]]
value = "1"

[[test.results]]
value = "true"

[[test.results]]
value = "'inner'"

[[test.results]]
value = "NONE"
*/

-- A FOR variable may not take a protected name.
FOR $session IN [{ data: 'inner' }] { RETURN $session.data };
FOR $auth IN [{ id: 1 }] { RETURN $auth };

-- A closure parameter may not take a protected name.
RETURN (|$session| $session.data)({ data: 'inner' });

-- A user-defined function argument may not take a protected name, at DEFINE.
DEFINE FUNCTION fn::takes_token($token: any) { RETURN $token; };

-- A user-defined function argument may not take a protected name, at ALTER: the
-- function is defined with none, then altering it to add one is refused.
DEFINE FUNCTION fn::alterable() { RETURN 1; };
ALTER FUNCTION fn::alterable($auth: any) { RETURN $auth; };

-- A closure parameter bound after a catalog change in the same block, where the
-- bind is not eager, is refused when it is reached - here the map evaluates it -
-- with the protected-variable error, not an internal planning error.
{ DEFINE TABLE t; RETURN [1].map(|$auth| 1) };

-- A reachable protected closure is refused when planned eagerly.
RETURN (|$auth| 1);

-- A reachable protected closure is refused after a catalog change too, when the
-- arm that holds it runs.
{ DEFINE TABLE tr; IF true { RETURN (|$auth| 1) }; RETURN 1 };

-- An UNREACHABLE protected closure after a catalog change is not refused: the
-- IF arm never runs, so the closure is never reached, the statement succeeds,
-- and the DEFINE TABLE before it stands.
{ DEFINE TABLE tu; IF false { RETURN (|$auth| 1) }; RETURN 1 };
RETURN (INFO FOR DB).tables.tu != NONE;

-- A non-protected name still binds, and the body reads it.
FOR $row IN [{ data: 'inner' }] { RETURN $row.data };

-- An unshadowed read still resolves against the session, which carries no data.
RETURN $session.data;
