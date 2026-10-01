/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "`ALTER PARAM $p VALUE <expr>` stores a computed value into the param catalog exactly like DEFINE PARAM, and it is rendered back recursively by INFO/EXPORT, so it needs the same depth write guard. `$p` inside the new value resolves to the current stored value, so `ALTER PARAM $p VALUE [$p]` is a read-modify-write that grows the stored value one level per statement with no per-step guard. Here the param is first defined at the cap by an accepted fold, then a single `ALTER` wraps it once more, so it is the ALTER write guard - not the DEFINE guard or the fold cap - that rejects it, before a value past the cap can persist and later abort INFO/EXPORT. The failed ALTER leaves the stored value unchanged, so the param is still readable at the cap."

[[test.results]]
value = "NONE"

[[test.results]]
error = "The value is nested too deeply. A value may be nested at most 256 levels; reduce the nesting or split the data across records"

[[test.results]]
value = "1"

*/

DEFINE PARAM $p VALUE array::fold(array::range(0, 255), [], |$a, $_| [$a]);
ALTER PARAM $p VALUE [$p];
RETURN array::len($p);
