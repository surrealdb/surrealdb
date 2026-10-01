/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A top-level `LET` binds into the request-scoped context and is not reset between statements, so `LET $a = [$a]` chained across statements grows the bound value's nesting without bound - the same accumulation vector as fold/reduce, and equally fatal on the value's recursive clone. (A body `LET` inside a `FOR` is iteration-scoped and does not accumulate.) The binding is capped, so a top-level `LET` of a value past the cap is rejected rather than bound. The value is built at the cap by an accepted fold and then wrapped once more by a plain array literal - not a fold - so it is the `LET` binding guard, not the fold cap, that rejects it."

[[test.results]]
value = "NONE"

[[test.results]]
error = "The value is nested too deeply. A value may be nested at most 256 levels; reduce the nesting or split the data across records"

[[test.results]]
value = "true"

*/

LET $at_cap = array::fold(array::range(0, 255), [], |$a, $_| [$a]);
LET $too_deep = [$at_cap];
RETURN $too_deep IS NONE;
