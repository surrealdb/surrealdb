/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:1 }, { id: test:2 }, { id: test:3 }, { id: test:4 }]"

*/
DEFINE FUNCTION fn::break(){
	break;
};


FOR $test in 1..10{
	if $test == 5 {
		fn::break();
	};
	CREATE type::thing('test',$test);
};
SELECT * FROM test;
