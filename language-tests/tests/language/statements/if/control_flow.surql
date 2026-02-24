/**
[test]
reason = "Test control flow statements (RETURN, THROW) within IF/ELSE branches"

[[test.results]]
# RETURN from IF body
value = "'returned-early'"

[[test.results]]
# RETURN from ELSE body
value = "'returned-from-else'"

[[test.results]]
# RETURN from ELSE IF body
value = "'returned-from-elseif'"

[[test.results]]
# Block with IF containing RETURN - returns early
value = "'from-if'"

[[test.results]]
# THROW from IF body
error = "An error occurred: thrown error"

*/

-- RETURN from IF body
{
    IF true { RETURN "returned-early" };
    "not-reached"
};

-- RETURN from ELSE body
{
    IF false { "not-taken" } ELSE { RETURN "returned-from-else" };
    "not-reached"
};

-- RETURN from ELSE IF body
{
    IF false { "not-taken" } ELSE IF true { RETURN "returned-from-elseif" };
    "not-reached"
};

-- Block with IF containing RETURN - returns early
{
    LET $x = 10;
    IF $x > 5 { RETURN "from-if" };
    "not-reached"
};

-- THROW from IF body
IF true { THROW "thrown error" };
