/**
[env]
namespace = true
database = true

[test]
reason = "the planner folds $session from the ambient context, so a FOR loop, closure or function argument named $session must stop the fold"

[[test.results]]
value = "'inner'"

[[test.results]]
value = "'inner'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'inner'"

[[test.results]]
value = "NONE"
*/

-- `$session` is refused by `LET`, `UNSET` and the `set` RPCs, so no
-- statement rebinds it and the planner may read it out of the context the
-- plan will run in. The constructs that bind a user-chosen parameter name
-- bind it unchecked, though, and a read beneath one of them is the value
-- they bind — so each of these answers with the binding, not the session.

FOR $session IN [{ data: 'inner' }] { RETURN $session.data };

RETURN (|$session| $session.data)({ data: 'inner' });

DEFINE FUNCTION fn::takes_session($session: any) { RETURN $session.data; };

RETURN fn::takes_session({ data: 'inner' });

-- The unshadowed read still resolves against the session itself, which
-- carries no `data`.
RETURN $session.data;
