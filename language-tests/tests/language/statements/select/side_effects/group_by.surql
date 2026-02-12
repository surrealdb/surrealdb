/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "2"

[[test.results]]
value = "4"

*/
{
	CREATE a:1 SET v = 1;
	CREATE a:2 SET v = 1;
	CREATE a:3 SET v = 2;
	CREATE a:4 SET v = 2;
	CREATE count:1 SET v = 0;
	CREATE count:2 SET v = 0;
	SELECT (UPDATE count:1 SET v += 1), v FROM a GROUP BY v;
	SELECT (UPDATE count:2 SET v += 1), (UPDATE count:2 SET v += 1), v FROM a GROUP BY v;
	"OK";
};
(count:1).v;
(count:2).v;
