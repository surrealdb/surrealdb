/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
error = "Incorrect arguments for function rand::int(). Expected 0 or 2 arguments"

[[test.results]]
error = "Incorrect arguments for function rand::int(). Argument 1 was the wrong type. Expected `int` but found `NULL`"

*/
let $a = (<array> 0..1000).map(|$_| rand::int());
$a.all(|$x| type::is::int($x));

let $b = (<array> 0..1000).map(|$_| rand::int(10,20));
$b.all(|$x| type::is::int($x));
$b.all(|$x| $x >= 10 && $x <= 20);


rand::int(10);
rand::int(NULL,10);
