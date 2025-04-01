/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: foo:0 }]"

*/

FOR $i in 0..3{
	if $i == 1 {
		for $j in 0..({ BREAK }){}
	};
	CREATE type::thing('foo',$i);
};
SELECT * FROM foo;
