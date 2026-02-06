/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "2"

*/


{
	CREATE a:1 SET v = 1;
	CREATE a:2 SET v = 1;
	CREATE a:3 SET v = 2;
	CREATE a:4 SET v = 2;
	CREATE count:1 SET v = 0;
	SELECT (UPDATE count:1 SET v += 1) FROM a WHERE v = 1;
	"OK";
};
(count:1).v;
