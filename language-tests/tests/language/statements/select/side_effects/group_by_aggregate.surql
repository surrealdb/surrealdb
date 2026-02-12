/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "5"

*/
{
	CREATE a:1 SET v = 1;
	CREATE a:2 SET v = 1;
	CREATE a:3 SET v = 2;
	CREATE a:4 SET v = 2;
	CREATE a:5 SET v = 3;
	CREATE count:1 SET v = 0;
	SELECT math::sum((UPDATE ONLY count:1 SET v += 1 RETURN VALUE v)), v FROM a GROUP BY v;
	"OK";
};
(count:1).v;
