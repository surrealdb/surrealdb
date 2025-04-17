/**
[env]
timeout = 5000

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

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
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
error = "Incorrect arguments for function rand::guid(). Argument 1 was the wrong type. Expected `int` but found `NULL`"

*/
let $a = (<array> 0..1000).map(|$_| rand::guid());
$a.all(|$x| type::is::string($x));
$a.all(|$x| $x = /[a-z0-9]*/);
$a.all(|$x| $x.len() == 20 );

let $b = (<array> 0..1000).map(|$_| rand::guid(10,20));
$b.all(|$x| type::is::string($x));
$b.all(|$x| $x = /[a-z0-9]*/);
$b.all(|$x| $x.len() >= 10 && $x.len() <= 20);

let $b = (<array> 0..1000).map(|$_| rand::guid(10));
$b.all(|$x| type::is::string($x));
$b.all(|$x| $x = /[a-z0-9]*/);
$b.all(|$x| $x.len() == 10);

rand::guid(NULL,10);
