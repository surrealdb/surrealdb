/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "ORDER BY DESC + variable LIMIT inside DEFINE FUNCTION must return the correct (newest) row"
issue = 7130

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ conversation: 'c1', id: bug_msg:1, seq: 1 }]"

[[test.results]]
value = "[{ conversation: 'c1', id: bug_msg:2, seq: 2 }]"

[[test.results]]
value = "[{ conversation: 'c1', id: bug_msg:3, seq: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ conversation: 'c1', id: bug_msg:3, seq: 3 }]"

[[test.results]]
value = "[{ conversation: 'c1', id: bug_msg:3, seq: 3 }]"

[[test.results]]
value = "[{ conversation: 'c1', id: bug_msg:3, seq: 3 }]"
*/

DEFINE TABLE OVERWRITE bug_msg SCHEMAFULL;
DEFINE FIELD OVERWRITE conversation ON bug_msg TYPE string;
DEFINE FIELD OVERWRITE seq ON bug_msg TYPE int;
DEFINE INDEX OVERWRITE bug_msg_seq_idx ON bug_msg FIELDS conversation, seq UNIQUE;

CREATE bug_msg:1 SET conversation = "c1", seq = 1;
CREATE bug_msg:2 SET conversation = "c1", seq = 2;
CREATE bug_msg:3 SET conversation = "c1", seq = 3;

-- Exact reproduction from issue: IF/LET nesting + variable $limit
DEFINE FUNCTION OVERWRITE fn::test_range($conv: string, $limit: int) {
    LET $r = IF true {
        LET $inner = SELECT * FROM bug_msg
            WHERE conversation = $conv
                AND seq >= 0
            ORDER BY seq DESC
            LIMIT $limit;
        $inner;
    };
    RETURN $r;
};

-- Equality-only variant with same nesting
DEFINE FUNCTION OVERWRITE fn::test_equality($conv: string, $limit: int) {
    LET $r = IF true {
        LET $inner = SELECT * FROM bug_msg
            WHERE conversation = $conv
            ORDER BY seq DESC
            LIMIT $limit;
        $inner;
    };
    RETURN $r;
};

-- CASE 1: range predicate in function (should return seq=3)
fn::test_range("c1", 1);

-- CASE 2: equality predicate in function (should return seq=3)
fn::test_equality("c1", 1);

-- CASE 3: top-level with literal LIMIT (should return seq=3, baseline)
SELECT * FROM bug_msg WHERE conversation = "c1" AND seq >= 0 ORDER BY seq DESC LIMIT 1;
