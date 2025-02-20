/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

*/
let $array = (<array> 0..10000).map(|$x| rand::bool());
let $sum = $array.fold(0,|$acc,$x| $acc + if $x {  1 } else { 0 });
// Should pretty much always be true.
// Tests that there is atleast some true and false statements.
$sum > 1000 && $sum < 9000;
$array.all(|$x| type::is::bool($x));

