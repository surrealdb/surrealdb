/**
[test]
reason = "Test that RETURN breaks nested execution in functions, transactions, blocks, and conditionals"

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
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "[2, 2, 4, 4]"

*/

-- Test RETURN in function definition - should return 1, not 2
DEFINE FUNCTION fn::test() {
    {
        RETURN 1;
    };
    RETURN 2;
};

RETURN fn::test();

-- Test RETURN in transaction - should stop after RETURN, not execute CREATE a:2
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

