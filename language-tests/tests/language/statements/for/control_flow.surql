/**
[test]
reason = "Test control flow statements (RETURN, BREAK, CONTINUE) within FOR loops"

[[test.results]]
# RETURN from FOR loop body
value = "'found-it'"

[[test.results]]
# BREAK exits the loop early
value = "NONE"

[[test.results]]
# Verify BREAK effect - only first two items processed
value = "[{ id: cf_test:[1], val: 1 }, { id: cf_test:[2], val: 2 }]"

[[test.results]]
# Clean up
value = "[]"

[[test.results]]
# CONTINUE skips current iteration
value = "NONE"

[[test.results]]
# Verify CONTINUE effect - item 2 was skipped
value = "[{ id: cf_test:[1], val: 1 }, { id: cf_test:[3], val: 3 }]"

[[test.results]]
# Nested FOR with BREAK only exits inner loop
value = "'completed'"

[[test.results]]
# RETURN from nested FOR exits all loops
value = "'early-exit'"

*/

-- RETURN from FOR loop body
{
    FOR $i IN [1, 2, 3] {
        IF $i == 2 { RETURN "found-it" };
    };
    "not-reached"
};

-- BREAK exits the loop early
FOR $i IN [1, 2, 3, 4, 5] {
    UPSERT cf_test:[$i] SET val = $i;
    IF $i == 2 { BREAK };
};

-- Verify BREAK effect - only first two items processed
SELECT * FROM cf_test ORDER BY id;

-- Clean up and test CONTINUE
DELETE cf_test;

-- CONTINUE skips current iteration
FOR $i IN [1, 2, 3] {
    IF $i == 2 { CONTINUE };
    UPSERT cf_test:[$i] SET val = $i;
};

-- Verify CONTINUE effect - item 2 was skipped
SELECT * FROM cf_test ORDER BY id;

-- Nested FOR with BREAK only exits inner loop
{
    FOR $outer IN [1, 2] {
        FOR $inner IN [1, 2, 3] {
            IF $inner == 2 { BREAK };
        };
    };
    "completed"
};

-- RETURN from nested FOR exits all loops
{
    FOR $outer IN [1, 2] {
        FOR $inner IN [1, 2, 3] {
            IF $inner == 2 AND $outer == 1 { RETURN "early-exit" };
        };
    };
    "not-reached"
};
