/**
[test]
reason = "Test idiom function calls with computed arguments"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "123"

[[test.results]]
value = "456"
*/

LET $str = "abc";
"abcdef".starts_with($str);

LET $obj = {
	a: |$a: int| $a,
	b: |$b: function| $b()
};

LET $num = 123;
LET $fnc = || 456;

$obj.a($num);
$obj.b($fnc);
