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
let $a = 1;
$a;
{
	let $a = 2;
	let $b = 3;
	{a: $a, b: $b};
};
$a;
$b;
