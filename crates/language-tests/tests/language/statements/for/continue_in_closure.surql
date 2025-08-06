/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
value = "[]"

*/

DEFINE DATABASE test;

FOR $test in 1..10{
	if $test == 5 {
		(|| { continue; })();
	};
	CREATE type::thing('test',$test);
};
SELECT * FROM test;
