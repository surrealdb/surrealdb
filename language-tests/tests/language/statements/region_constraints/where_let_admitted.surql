/**
[test]
reason = "Register #28, row ConditionClause, admitted control: a LET in a block that stands as a statement is admitted. Statement two of language/statements/block/basic.surql, with its result."

[[test.results]]
value = "6"
*/
{
	LET $x = 5;
	$x + 1
};
