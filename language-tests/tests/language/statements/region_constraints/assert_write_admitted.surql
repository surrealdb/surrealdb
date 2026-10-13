/**
[test]
reason = "Register #28, row PermissionOrAssertFrame, admitted control: the ASSERT clause of a field that holds no write is admitted. A copy of language/closure/matrix/Q_field_assert_plain_let.surql."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: 1, id: w:1 }]"

[[test.results]]
value = "[1]"
*/
LET $m = 8;
DEFINE FIELD f ON w ASSERT $value >= $m;
CREATE w:1 SET f = 1;
SELECT VALUE f FROM w;
