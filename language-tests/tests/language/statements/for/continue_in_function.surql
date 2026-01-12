/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "The table 'test' does not exist"

*/
DEFINE FUNCTION fn::continue(){
	continue;
};


FOR $test in 1..10{
	if $test == 5 {
		fn::continue();
	};
	CREATE type::record('test',$test);
};
SELECT * FROM test;
