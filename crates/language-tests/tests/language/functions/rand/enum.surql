/**
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


*/
let $array = (<array> 0..1000).map(|$x| rand::enum("a","b","c"));
$array.all(|$x| $x in ["a","b","c"]);

BEGIN;
let $len_a = $array.filter(|$x| $x == "a").len();
let $len_b = $array.filter(|$x| $x == "b").len();
let $len_c = $array.filter(|$x| $x == "c").len();
// Ensure the the picks produced contain atleast some variety.
// Ranges are made very large to ensure this is very unlikely to be untrue
RETURN $len_a > 100 && $len_a < 1000 / 3 * 2.5
	&& $len_b > 100 && $len_b < 1000 / 3 * 2.5
	&& $len_c > 100 && $len_c < 1000 / 3 * 2.5;
COMMIT;


let $array = (<array> 0..1000).map(|$x| rand::enum(["a","b","c"]));
$array.all(|$x| $x in ["a","b","c"]);

BEGIN;
let $len_a = $array.filter(|$x| $x == "a").len();
let $len_b = $array.filter(|$x| $x == "b").len();
let $len_c = $array.filter(|$x| $x == "c").len();
// Ensure the the picks produced contain atleast some variety.
// Ranges are made very large to ensure this is very unlikely to be untrue
RETURN $len_a > 100 && $len_a < 1000 / 3 * 2.5
	&& $len_b > 100 && $len_b < 1000 / 3 * 2.5
	&& $len_c > 100 && $len_c < 1000 / 3 * 2.5;
COMMIT;
