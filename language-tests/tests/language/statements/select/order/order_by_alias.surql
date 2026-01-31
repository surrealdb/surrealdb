/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ ord: 1 }, { ord: 2 }, { ord: 3 }, { ord: 4 }, { ord: 5 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ ord2: 1 }, { ord2: 2 }, { ord2: 3 }, { ord2: 4 }, { ord2: 5 }]"

*/

{
	CREATE foo:1 SET v = 1;
	CREATE foo:2 SET v = 2;
	CREATE foo:3 SET v = 3;
	CREATE foo:4 SET v = 4;
	CREATE foo:5 SET v = 5;
	RETURN "OK";
};

SELECT v as ord FROM foo ORDER BY ord;

DEFINE FUNCTION fn::identity($v: any){
	return $v
};

SELECT fn::identity(v) as ord2 FROM foo ORDER BY ord2;
