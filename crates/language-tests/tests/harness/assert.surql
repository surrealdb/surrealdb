/**
[test]
run = false

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/

DEFINE FUNCTION fn::assert($v: any, $message: option<string>){
    if (!$v){
        THROW $message
    }
};

DEFINE FUNCTION fn::assert_eq($a: any,$b:any, $message: option<string>){
    if ($a != $b){
        THROW (<string> $a) + " != " + (<string> $b) + " " + $message
    }
};

