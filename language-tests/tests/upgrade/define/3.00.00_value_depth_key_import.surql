/**
[test]
reason = """
Setup for value_depth_key.surql: the old binary stores one record whose compound
id nests far past the depth the reading binary will walk, plus a control record
whose compound id nests well within it.

The nesting is in the record id, so it lives in the storage key rather than in
the stored value. Both rows carry a plain `marker` field, which is how the body
addresses them without ever projecting the id that crosses the bound.

`RETURN NONE` keeps the deep id off the wire: the import is graded only on
whether its statements error."""
run = false
*/
DEFINE TABLE up_vdepth_key SCHEMALESS;

-- The control: a compound id nested well within any bound a reader applies.
CREATE type::record('up_vdepth_key', [1]) SET marker = 'shallow';

-- The violation: one array level per fold step, 301 in all, as the record id.
CREATE type::record('up_vdepth_key', array::fold(array::range(0, 300), [], |$acc, $item| [$acc])) SET marker = 'deep' RETURN NONE;
