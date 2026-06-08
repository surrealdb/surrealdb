/**
[env]
namespace = true
database = true
# Targets the legacy compute path (`value::fetch`). The new streaming planner
# uses its own FETCH implementation that doesn't currently support index parts
# in idiom paths, so it would fail the positive-index assertion regardless of
# this fix.
planner-strategy = ["compute-only"]

[test]
reason = "FETCH clause with a negative or out-of-range array index used to rely on `i64 as usize` truncation, which silently produced a huge `usize` so `array.get_mut` returned `None`. On 64-bit platforms this happened to do the right thing, but the cast also lost information on 32-bit and made the intent unreadable. The fix replaces the cast with `usize::try_from` so negative indices explicitly skip the fetch. Behaviour for positive in-range indices is unchanged."

[[test.results]]
value = "[{ id: other:1, name: 'A' }]"

[[test.results]]
value = "[{ id: other:2, name: 'B' }]"

[[test.results]]
value = "[{ id: other:3, name: 'C' }]"

[[test.results]]
value = "[{ id: foo:x, refs: [other:1, other:2, other:3] }]"

[[test.results]]
value = "[{ id: foo:x, refs: [{ id: other:1, name: 'A' }, other:2, other:3] }]"

[[test.results]]
value = "[{ id: foo:x, refs: [other:1, other:2, other:3] }]"

[[test.results]]
value = "[{ id: foo:x, refs: [other:1, other:2, other:3] }]"
*/

CREATE other:1 SET name = 'A';
CREATE other:2 SET name = 'B';
CREATE other:3 SET name = 'C';
CREATE foo:x SET refs = [other:1, other:2, other:3];

-- Positive in-range index: only the indexed element is fetched.
SELECT * FROM foo:x FETCH refs[0];

-- Negative index: skip silently, no element is fetched.
SELECT * FROM foo:x FETCH refs[-1];

-- Out-of-range positive index: skip silently, no element is fetched.
SELECT * FROM foo:x FETCH refs[10];
