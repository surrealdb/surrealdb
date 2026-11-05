/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A schema statement's clauses are expressions, and a construct the logical layer
has no form for is evaluated wherever it appears —
including in the clause a schema statement is formed from. A mock record set
outside `FROM` is one such construct, so `DEFINE PARAM ... VALUE |t:1..3|`
resolves rather than failing.

The set of declined constructs belongs to lowering; a schema statement accepts
what an expression accepts and narrows nothing.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[t:1, t:2, t:3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "3"
*/

DEFINE PARAM $ids VALUE |t:1..=3|;
RETURN $ids;

DEFINE PARAM $count VALUE array::len(|t:1..=3|);
RETURN $count;
