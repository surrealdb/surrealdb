/**
[test]
reason = """
A RETURN ends the innermost value scope it stands in: a function body's inner
block, a statement of a transaction, a block expression, or a conditional in a
projection. A RETURN directly inside BEGIN … COMMIT is a value statement, so the
statements after it run (register #8). The function case needs the value-scope
RETURN lowering of lane L9 and passes only once it merges.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

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

-- Test RETURN in function definition - the inner block catches its RETURN, so the function returns 2
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

