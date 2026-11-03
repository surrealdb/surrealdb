/**
[test]
reason = """
A RETURN ends the context it stands in: a function body, whatever block in it
holds the RETURN, a statement of a transaction, a top-level statement that is a
block, or the projection of a row. A RETURN directly inside BEGIN … COMMIT ends
its own statement, so the statements after it run.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ id: a:1 }"

[[test.results]]
value = "1"

[[test.results]]
value = "{ id: a:2 }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "[2, 2, 4, 4]"

*/

-- Test RETURN in function definition - a block is no context, so the inner RETURN ends the function with 1
DEFINE FUNCTION fn::test() {
    {
        RETURN 1;
    };
    RETURN 2;
};

RETURN fn::test();

-- Test RETURN in transaction - a value statement, so CREATE a:2 still runs
BEGIN;
CREATE ONLY a:1;
RETURN 1;
CREATE ONLY a:2;
COMMIT;

-- Test RETURN in block expression
{
    RETURN 1;
};

-- Test RETURN in IF expressions within SELECT VALUE
SELECT VALUE {
    IF $this % 2 == 0 {
        RETURN $this;
    } ELSE {
        RETURN $this + 1;
    }
} FROM [1, 2, 3, 4];

