/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: foo:0 }]"

*/

FOR $i in 0..3{
	if $i == 1 {
		[].all(({ BREAK }))
	};
	CREATE type::thing('foo',$i);
};
SELECT * FROM foo;
