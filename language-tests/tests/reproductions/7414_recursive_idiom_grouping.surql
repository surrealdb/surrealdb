/**
[test]
reason = "Preserve recursive idiom grouping when stored function bodies are serialized."
results = [
    "[]", "[]", "[]", "NONE", "NONE", "NONE",
    "[2, 3]", "[2, 3]", "[2, 3]",
    "NONE",
    "[[3, 2], n:2, [n:3], [], [2, 3], [2]]",
    "[[3, 2], n:2, [n:3], [], [2, 3], [2]]",
]
*/

CREATE |n:1..4| RETURN NONE;
RELATE n:1->links->n:2 RETURN NONE;
RELATE n:2->links->n:3 RETURN NONE;

DEFINE FUNCTION fn::idiom_direct($node: record<n>) {
    RETURN ($node.{..+collect}->links->n).map(|$v| record::id($v));
};

DEFINE FUNCTION fn::idiom_indirect($node: record<n>) {
    LET $ids = $node.{..+collect}->links->n;
    RETURN $ids.map(|$v| record::id($v));
};

LET $node = n:1;
fn::idiom_direct($node);
fn::idiom_indirect($node);
($node.{..+collect}->links->n).map(|$v| record::id($v));

DEFINE FUNCTION fn::idiom_scopes($node: record<n>) {
    RETURN [
        (($node.{..+collect}->links->n).map(|$v| record::id($v))).reverse(),
        ($node.{..+collect}->links->n)[0],
        // Filtering the collected result still reaches n:3 through n:2.
        ($node.{..+collect}->links->n).filter(|$v| $v != n:2),
        // Filtering inside recursion stops traversal at n:2.
        $node.{..+collect}->links->n.filter(|$v| $v != n:2),
        $node.{..+collect}(->links->n).map(|$v| record::id($v)),
        ($node.{1+collect}->links->n).map(|$v| record::id($v))
    ];
};

fn::idiom_scopes($node);
fn::idiom_scopes($node);
