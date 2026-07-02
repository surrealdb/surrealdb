/**
[env]
namespace = true
database = true
# Pinned to the strategies where the new streaming executor's FETCH
# implementation actually runs: `exec/operators/fetch.rs` only handles
# `Part::Field`/`Part::All`/`Part::First`/`Part::Last` in a FETCH idiom path;
# any other part (a numeric index, positive or negative) falls through a
# catch-all `_ => Ok(())` and silently fetches nothing. Under `best-effort-ro`
# (the production default) this is not a fallback: no error is raised, so the
# wrong (unfetched) result is returned silently in production too, not just
# under the strict `all-ro` test strategy.
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
wip = true
reason = "Parity sibling of fetch_array_index_negative.surql (#472, B4): asserts the same legacy/correct FETCH results for a positive in-range numeric index against the new executor. Currently red: `refs[0]` should fetch only that element (as compute-only does) but the new executor's FETCH silently fetches nothing for any numeric index. Marked wip so this failure is tracked as a warning rather than blocking CI; remove `wip` (and delete this pin, merging back into the sibling file) once #473 lands."
issue = 473

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

-- Positive in-range index: only the indexed element should be fetched.
-- STILL BROKEN under all-ro/best-effort-ro: the new executor's FETCH fetches
-- nothing for a numeric index part, so this currently returns the fully
-- unfetched `refs` array instead of the expected value above.
SELECT * FROM foo:x FETCH refs[0];

-- Negative index: skip silently, no element is fetched. This coincidentally
-- matches legacy already (the new executor's catch-all also no-ops here),
-- so it's not itself evidence of a fix -- kept for structural parity with
-- the compute-only sibling.
SELECT * FROM foo:x FETCH refs[-1];

-- Out-of-range positive index: skip silently, no element is fetched. Same
-- coincidental-match caveat as the negative-index case above.
SELECT * FROM foo:x FETCH refs[10];
