/**
[test]

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
error = "Incorrect arguments for function set::any(). Argument 1 was the wrong type. Expected `set` but found `'some text'`"

[[test.results]]
value = "true"


*/
RETURN set::any({,});
RETURN set::any({false,});
RETURN set::any({NULL,});
RETURN set::any({NONE,});
RETURN set::any({{,}});

RETURN set::any({true,});
RETURN set::any("some text");
RETURN set::any({1,2,"text",3,NONE,3,4});
