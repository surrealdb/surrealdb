/**
[test]
reason = "A table's select permission is a clause with no effects, so an evaluation count cannot be observed from SurrealQL. Not reproducible as reported: the guard is judged once per row as a column or filter of the plan."
issue = 5657
run = false
*/

RETURN NONE;
