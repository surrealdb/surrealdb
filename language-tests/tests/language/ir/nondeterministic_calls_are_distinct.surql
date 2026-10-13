/**
[env]
namespace = true
database = true

[test]
reason = """
Two calls of a non-deterministic function with equal text are two draws: a sequence
advances twice and two generated UUIDs differ.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[0, 1]"

[[test.results]]
value = "false"
*/

DEFINE SEQUENCE s;
RETURN [sequence::nextval('s'), sequence::nextval('s')];
RETURN rand::uuid::v7() = rand::uuid::v7();
