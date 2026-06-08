/**
[env]
# Compute-only routes through a separate (legacy) code path with its
# own, unrelated divergences for `->knows.out`; this test pins parity
# between the two streaming planner strategies (`all-ro` and
# `best-effort-ro`), which is the scope of the bug being verified.
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = "Unaliased graph-traversal field expressions (e.g. `->knows.out`) must produce the same nested output shape in both the projection and aggregation streaming planner paths. `plan_projections` / `plan_projections_fast` route through `idiom_to_field_path`, which pushes a `FieldPathPart::Lookup` entry for the Lookup part; `plan_aggregation` routes through `alias_output_path`, which must do the same. Without this the non-GROUP-BY path nests into `{ \"->knows\": { out: _ } }` while GROUP BY produces the flat key `\"->knows.out\"`."

[[test.results]]
value = "[{ age: 30, id: person:a }]"

[[test.results]]
value = "[{ age: 30, id: person:b }]"

[[test.results]]
value = "[{ age: 40, id: person:c }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

# Non-GROUP-BY: each person's `->knows.out` nests under "->knows".
[[test.results]]
value = '[{ "->knows": { out: [person:c] }, id: person:a }, { "->knows": { out: [person:c] }, id: person:b }, { "->knows": { out: [] }, id: person:c }]'

# GROUP BY age: same nested shape for "->knows.out", with per-group
# aggregated `out` arrays. Would previously be `{ "->knows.out": … }`
# (flat) without the `Part::Lookup` arm in `alias_output_path`.
[[test.results]]
value = '[{ "->knows": { out: [[person:c], [person:c]] }, age: 30 }, { "->knows": { out: [[]] }, age: 40 }]'
*/

CREATE person:a SET age = 30;
CREATE person:b SET age = 30;
CREATE person:c SET age = 40;
LET $_ = (RELATE person:a->knows->person:c);
LET $_ = (RELATE person:b->knows->person:c);

-- Non-GROUP-BY streaming projection path (`idiom_to_field_path`):
-- `->knows.out` nests into `{ "->knows": { out: [...] } }`.
-- Each person has at most one outgoing `knows` edge so array order is
-- deterministic across executor strategies.
SELECT id, ->knows.out FROM person ORDER BY id;

-- GROUP BY streaming aggregation path (`alias_output_path`) must
-- produce the same nested shape for `->knows.out` as the non-GROUP-BY
-- path, not a flat `"->knows.out"` key.
SELECT age, ->knows.out FROM person GROUP BY age ORDER BY age;
