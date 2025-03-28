/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: foo:0 }, { id: foo:2 }]"

*/

FOR $i in 0..3{
	if $i == 1 {
		1 + ({ CONTINUE })
	};
	CREATE type::thing('foo',$i);
};
SELECT * FROM foo;
