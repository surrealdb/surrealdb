
FOR $i in 0..3{
	if $i == 1 {
		array::all(({ BREAK }))
	};
	CREATE type::thing('foo',$i);
};
SELECT * FROM foo;
