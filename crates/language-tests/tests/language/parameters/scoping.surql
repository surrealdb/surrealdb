/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "{ a: 2, b: 3 }"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

*/
$a = 1;
$a;
{
	$a = 2;
	$b = 3;
	{a: $a, b: $b};
};
$a;
$b;
