/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A parenthesised recursive graph idiom followed by .map inside a function.
"""
issue = 7414

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "[2, 3]"

*/
CREATE |n:1..4| RETURN NONE;
RELATE n:1->links->n:2 RETURN NONE;
RELATE n:2->links->n:3 RETURN NONE;
DEFINE FUNCTION OVERWRITE fn::idiom_direct($node: record<n>) {
    RETURN ($node.{..+collect}->links->n).map(|$v| record::id($v))
};
DEFINE FUNCTION OVERWRITE fn::idiom_indirect($node: record<n>) {
    LET $ids = $node.{..+collect}->links->n;
    RETURN $ids.map(|$v| record::id($v));
};
LET $node = n:1;
fn::idiom_direct($node);
fn::idiom_indirect($node);
($node.{..+collect}->links->n).map(|$v| record::id($v));
