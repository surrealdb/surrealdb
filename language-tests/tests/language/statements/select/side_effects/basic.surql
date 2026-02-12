/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "4"

*/

{
	CREATE a:1;
	CREATE a:2;
	CREATE a:3;
	CREATE a:4;
	CREATE count:1 SET v = 0;
	SELECT (UPDATE count:1 SET v += 1) FROM a;
	"OK";
};
(count:1).v;

