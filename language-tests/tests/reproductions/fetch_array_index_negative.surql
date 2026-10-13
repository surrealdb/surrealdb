/**
[env]
namespace = true
database = true

[test]
reason = "FETCH clause with a negative or out-of-range array index used to rely on `i64 as usize` truncation, which silently produced a huge `usize` so `array.get_mut` returned `None`. On 64-bit platforms this happened to do the right thing, but the cast also lost information on 32-bit and made the intent unreadable. The fix replaces the cast with `usize::try_from` so negative indices explicitly skip the fetch. Behaviour for positive in-range indices is unchanged. Also verifies the new streaming executor's FETCH operator, which previously (#472/#473) silently fetched nothing for any numeric index -- runs under all three planner strategies now that it correctly evaluates and fetches a positive in-range index."

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

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: foo:x, refs: [other:1, { id: other:2, name: 'B' }, other:3] }]"

[[test.results]]
value = "[{ id: foo:y, obj: { k: other:3 } }]"

[[test.results]]
value = "[{ id: foo:y, obj: { k: { id: other:3, name: 'C' } } }]"
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

-- Dynamic (parameter) index: resolved at fetch time; the addressed element is
-- fetched.
LET $i = 1;
SELECT * FROM foo:x FETCH refs[$i];

-- String key on an object: descends into the keyed entry and fetches it.
CREATE foo:y SET obj = { k: other:3 };
SELECT * FROM foo:y FETCH obj['k'];
