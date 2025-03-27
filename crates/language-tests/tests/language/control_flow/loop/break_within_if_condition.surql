
FOR $i in 0..3{
	if $i == 1 {
		if ({ BREAK }) {}
	};
	CREATE type::thing('foo',$i);
};
SELECT * FROM foo;
