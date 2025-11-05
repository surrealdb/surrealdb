/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "1"

[[test.results]]
value = "['fnc', 'keys']"

[[test.results]]
value = "2"
*/

LET $obj = { 
    fnc: || 1, 
    keys: || 2 
};

$obj.fnc();
($obj.fnc)();

// built-in idiom methods get priority
$obj.keys();
($obj.keys)();