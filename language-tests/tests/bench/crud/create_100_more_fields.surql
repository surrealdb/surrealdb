/**
[bench]
run = true

[test]

[[test.results]]
value = "[]"

*/

create |table:100|
	SET a = rand(),
		b = rand::bool(),
		c = rand::duration(0s,1y),
		d = rand::id(),
		e = rand::string(),
		f = rand::time(),
		g = rand::uuid()
	RETURN NONE;
