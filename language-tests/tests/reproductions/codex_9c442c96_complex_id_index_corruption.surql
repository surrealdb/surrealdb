/**
[test]
reason = "Updating a record on a table with an index whose column is `id.<field>` (a component of a complex record id) must keep the index in sync. Before Codex finding 9c442c96 was fixed, `id.val` resolution on a complex id ran SELECT * against the already-stored new record while computing both `o` and `n` during `store_index_data`, so `o != n` was false and the old `id.val` entries were never removed — leaving the index pointing at the pre-update component."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: tenanted:{ tenant: 'a', val: 'x' } }]"

[[test.results]]
value = "[{ id: tenanted:{ tenant: 'a', val: 'y' } }]"

[[test.results]]
value = "[tenanted:{ tenant: 'a', val: 'x' }]"

[[test.results]]
value = "[tenanted:{ tenant: 'a', val: 'y' }]"

*/

DEFINE TABLE tenanted SCHEMALESS;
-- The index is on a component of the complex record id, not on a
-- document field.
DEFINE INDEX idx_val ON tenanted COLUMNS id.val;

CREATE tenanted:{ tenant: 'a', val: 'x' };
CREATE tenanted:{ tenant: 'a', val: 'y' };

-- WITH NOINDEX forces a table scan that re-evaluates `id.val` against
-- each stored record. This is the "ground truth" the index must agree
-- with. Before the fix, `id.val` on a complex id went through a
-- permission-less SELECT * back to the same record, returning the
-- doc-level `val` (none here, since we never wrote one) instead of the
-- id key component — so the table scan itself disagreed with what the
-- user wrote into the key.
SELECT VALUE id FROM tenanted WITH NOINDEX WHERE id.val = 'x';
SELECT VALUE id FROM tenanted WITH NOINDEX WHERE id.val = 'y';
