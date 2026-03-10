/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
value = "[]"

*/

DEFINE TABLE test;

FOR $test in 1..10{
	if $test == 5 {
		(|| { break; })();
	};
	CREATE type::record('test',$test);
};
SELECT * FROM test;
