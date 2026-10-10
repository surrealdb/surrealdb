/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A statement whose result cannot be converted to a public value (here a closure, which has no public representation) must fail only its own result, not propagate the conversion error out of the query and wipe the sibling statements' results too. A conversion error lands in the failing statement's own result slot, so the statements before and after it keep theirs."

[[test.results]]
value = "1"

[[test.results]]
error = "Closure values cannot be converted to public value"

[[test.results]]
value = "3"

*/

RETURN 1;
RETURN |$x| $x;
RETURN 3;
