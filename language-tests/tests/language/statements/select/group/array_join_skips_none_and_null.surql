/**
[test]
reason = "array::join in a grouped select joins the values a group holds and skips NONE and NULL, which have no text"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ j: 'a-c' }]"
*/

{
	CREATE joined:1 SET v = 'a';
	CREATE joined:2 SET other = 1;
	CREATE joined:3 SET v = NULL;
	CREATE joined:4 SET v = 'c';
	RETURN "OK";
};

SELECT array::join(v, '-') AS j FROM joined GROUP ALL;
