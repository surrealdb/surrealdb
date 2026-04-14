/**
[env]
namespace = "test"
database = "test"
timeout = 15000

[test]
reason = "Deeply nested stored function chains (10 levels) should not cause stack overflow"
issue = 7238

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'start:L1:L2:L3:L4:L5:L6:L7:L8:L9:L10'"
*/

DEFINE FUNCTION OVERWRITE fn::level_10($input: string) { RETURN string::concat($input, ":L10"); };
DEFINE FUNCTION OVERWRITE fn::level_9($input: string) { RETURN fn::level_10(string::concat($input, ":L9")); };
DEFINE FUNCTION OVERWRITE fn::level_8($input: string) { RETURN fn::level_9(string::concat($input, ":L8")); };
DEFINE FUNCTION OVERWRITE fn::level_7($input: string) { RETURN fn::level_8(string::concat($input, ":L7")); };
DEFINE FUNCTION OVERWRITE fn::level_6($input: string) { RETURN fn::level_7(string::concat($input, ":L6")); };
DEFINE FUNCTION OVERWRITE fn::level_5($input: string) { RETURN fn::level_6(string::concat($input, ":L5")); };
DEFINE FUNCTION OVERWRITE fn::level_4($input: string) { RETURN fn::level_5(string::concat($input, ":L4")); };
DEFINE FUNCTION OVERWRITE fn::level_3($input: string) { RETURN fn::level_4(string::concat($input, ":L3")); };
DEFINE FUNCTION OVERWRITE fn::level_2($input: string) { RETURN fn::level_3(string::concat($input, ":L2")); };
DEFINE FUNCTION OVERWRITE fn::level_1($input: string) { RETURN fn::level_2(string::concat($input, ":L1")); };

-- This call chain goes 10 levels deep
RETURN fn::level_1("start");
