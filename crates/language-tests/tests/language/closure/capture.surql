/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['three', 'two']"

*/

let $a = 1;
(||{ $a })();
$a;

LET $words = ["one", "two", "three"];

["four", "three", "two"].filter(|$w| $w IN $words);