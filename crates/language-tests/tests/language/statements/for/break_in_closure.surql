/**
[test]

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
value = "[]"

*/

FOR $test in 1..10{
	if $test == 5 {
		(|| { break; })();
	};
	CREATE type::thing('test',$test);
};
SELECT * FROM test;
