/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ test: "DEFINE FUNCTION fn::test($first: string, $last: string) { RETURN $first + $last } COMMENT 'test comment' PERMISSIONS FULL" }'''

*/
LET $comment = "test comment";

DEFINE FUNCTION fn::test($first: string, $last: string) {
    RETURN $first + $last;
} COMMENT $comment;

(INFO FOR DB).functions;