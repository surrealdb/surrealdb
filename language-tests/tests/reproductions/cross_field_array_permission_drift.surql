/**
[test]
reason = "Two overlapping wildcard field-permissions on one array must each cut by element identity, not by a concrete index. A parent `items[*]` that compacts the array and a child `items[*].secret` judged against the same pre-filter snapshot shared a mutable result: the child's concrete index, derived before the parent removed elements, landed on the survivor that shifted into it, leaking a denied secret (GHSA-wjh3-m4pv-q6cp, an incomplete-fix follow-up to the #7356 within-field fix). The parent keeps n=0 and n=2; neither survivor's secret is 's1', so both secrets must be hidden. The negative control removes no parent elements, so only the allowed 's1' survives."

[[test.results]]
value = "{ id: nested:one, items: [{ n: 0 }, { n: 2 }] }"

[[test.results]]
value = "{ id: unfiltered:one, items: [{ n: 0 }, { n: 1, secret: 's1' }, { n: 2 }, { n: 3 }] }"

[env]
imports = ["reproductions/cross_field_array_permission_drift_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:guest" }

*/

-- Authenticated as record user person:guest.

-- The decisive case: the parent compaction shifts the n=2 element into the
-- index whose original child value ('s1') was allowed. Its secret 's2' must
-- still be cut. Pre-fix this returned { n: 2, secret: 's2' }.
SELECT * FROM ONLY nested:one;

-- Negative control: no parent compaction, so concrete indices stay stable and
-- only the explicitly allowed 's1' survives.
SELECT * FROM ONLY unfiltered:one;
