/**
[test]
reason = """
Setup for value_depth.surql: the old binary stores one record whose value is
nested far past the depth the reading binary will walk, plus a control record
nested well within it.

The nesting is grown by a fold rather than written as a literal because a
parser's depth caps bound a single parse, not the depth a stored value reaches
across statements: accumulating it is how a value on disk gets past a bound the
parse-time caps would otherwise appear to cover.

`RETURN NONE` keeps the accumulated value off the wire: the import is graded
only on whether its statements error, and the value itself is 301 levels of
brackets."""
run = false
*/
DEFINE TABLE up_vdepth_shallow SCHEMALESS;
DEFINE TABLE up_vdepth_deep SCHEMALESS;

-- The control: nested well within any bound a reader applies.
CREATE up_vdepth_shallow:1 SET v = [[[[[1]]]]];

-- The violation: one array level per fold step, 301 in all, held in a field of
-- an ordinary record.
CREATE up_vdepth_deep:1 SET v = [] RETURN NONE;
UPDATE up_vdepth_deep:1 SET v = array::fold(array::range(0, 300), [], |$acc, $item| [$acc]) RETURN NONE;
