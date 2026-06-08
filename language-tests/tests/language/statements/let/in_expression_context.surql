/**
[test]
reason = "LET appearing in expression context (array literal, object value, function argument) rejects with a clear user-facing error on both the legacy compute path and the streaming planner, instead of the old 'unreachable' or 'block or sequence operators' messages."

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

[[test.results]]
error = "Invalid statement: LET statements can only appear at the top level of a query or inside a block expression"

*/

RETURN [LET $x = 1];
RETURN { a: LET $x = 1 };
RETURN type::is_int(LET $x = 1);
