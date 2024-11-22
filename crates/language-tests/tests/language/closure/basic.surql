/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "4"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'abc'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'works'"

[[test.results]]
error = "There was a problem running the ANONYMOUS function. Expected this function to return a value of type string, but found 123"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'works'"

[[test.results]]
error = "Incorrect arguments for function ANONYMOUS(). Expected a value of type 'string' for argument $arg"

*/
LET $double = |$n: number| $n * 2;
$double(2);

LET $pipe = |$arg| $arg;
$pipe('abc');

LET $rettype = |$arg| -> string { $arg };
$rettype('works');
$rettype(123);

LET $argtype = |$arg: string| $arg;
$argtype('works');
$argtype(123);
