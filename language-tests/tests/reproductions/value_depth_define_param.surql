/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A stored param is rendered back recursively by INFO and EXPORT, so its value's nesting must be bounded at the write the same way a record's is. `StoredParamDefinition` is a plain revisioned encode with no depth guard of its own, so `DEFINE PARAM` gets the check explicitly. The value here is built at the cap by an accepted fold and then wrapped once more by a plain array literal (not a fold), so it is the DEFINE PARAM write guard - not the fold cap - that rejects it, before a value past the cap can persist and later abort INFO/EXPORT when it is rendered."

[[test.results]]
value = "NONE"

[[test.results]]
error = "The value is nested too deeply. A value may be nested at most 256 levels; reduce the nesting or split the data across records"

[[test.results]]
value = "{  }"

*/

LET $at_cap = array::fold(array::range(0, 255), [], |$a, $_| [$a]);
DEFINE PARAM $p VALUE [$at_cap];
RETURN (INFO FOR DB).params;
