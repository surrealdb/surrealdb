/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "4"

[[test.results]]
error = "There was a problem running the non_func() function. no such method found for the object type"

*/

let $a = { func: (|$a| $a + $a), non_func: 1 };
$a.func(2);
$a.non_func(2);
