/**
[env]
timeout = 2000

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
error = "Incorrect arguments for function rand::string(). Argument 1 was the wrong type. Expected `int` but found `NULL`"

*/

let $a = (<array> 0..1000).map(|$_| rand::string());
$a.all(|$x| type::is::string($x));
$a.all(|$x| $x.len() == 32 );

let $b = (<array> 0..1000).map(|$_| rand::string(10,20));
$b.all(|$x| type::is::string($x));
$b.all(|$x| $x.len() >= 10 && $x.len() <= 20);

let $b = (<array> 0..1000).map(|$_| rand::string(10));
$b.all(|$x| type::is::string($x));
$b.all(|$x| $x.len() == 10);

rand::string(NULL,10);
