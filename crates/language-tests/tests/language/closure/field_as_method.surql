/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "4"

[[test.results]]
value = "NONE"

*/

let $a = { func: (|$a| $a + $a), non_func: 1 };
$a.func(2);
$a.non_func(2);
