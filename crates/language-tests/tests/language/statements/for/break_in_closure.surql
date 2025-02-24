/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:1 }, { id: test:2 }, { id: test:3 }, { id: test:4 }]"

*/

FOR $test in 1..10{
	if $test == 5 {
		(|| { break; })();
	};
	CREATE type::thing('test',$test);
};
SELECT * FROM test;
