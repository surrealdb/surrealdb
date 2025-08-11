/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ mean: 2.5f }]"

*/

{
	CREATE foo SET a = 1;
	CREATE foo SET a = 2;
	CREATE foo SET a = 3;
	CREATE foo SET a = 4;
	NONE;
};
SELECT math::mean(a) as mean FROM foo GROUP ALL;
