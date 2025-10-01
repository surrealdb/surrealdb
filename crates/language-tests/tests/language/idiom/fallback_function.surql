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

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "1"

[[test.results]]
value = "'test'"

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

--------------------------------------

LET $rid = (CREATE ONLY test:1 SET fnc = || 1, tb = || 2).id;

$rid.fnc();
($rid.fnc)();

// built-in idiom methods get priority
$rid.tb();
($rid.tb)();