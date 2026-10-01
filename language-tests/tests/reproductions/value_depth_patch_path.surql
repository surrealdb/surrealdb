/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A JSON Patch op path is split from an arbitrary string with no length bound, and applying an op walks `Value::put` / `cut` / `pick` once per segment - each natively recursive with no `Stk`. A path of a few thousand segments overflows the host stack and aborts the process under `panic = 'abort'` while the value is being built, before the value-depth guard (which inspects a finished value) can ever see it. It is reachable by any authenticated caller via `UPDATE ... PATCH`, the `patch` function, or the RPC `patch` method, all of which route through `Value::patch`. The patch path is bounded to the value depth cap there, so an over-long path errors with `ValueTooDeep` instead of aborting."

[[test.results]]
value = "[{ id: foo:1 }]"

[[test.results]]
error = "The value is nested too deeply. A value may be nested at most 256 levels; reduce the nesting or split the data across records"

[[test.results]]
value = "[foo:1]"

*/

CREATE foo:1;
-- The segment count is far past the depth cap so the path is rejected before
-- any recursion. It is deliberately large: with the guard removed this many
-- segments overflows the stack and aborts the process (a smaller count is
-- caught after the fact by the storage guard on a generous stack, masking the
-- abort), so this size keeps the test a genuine regression for the guard.
UPDATE foo:1 PATCH [{ op: 'add', path: string::repeat('/a', 400000), value: 1 }];
SELECT VALUE id FROM foo;
