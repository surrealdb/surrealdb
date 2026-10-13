/**
[test]
reason = "Register #28, row FieldValueFrame, admitted control: the COMPUTED clause of a field that holds no write is admitted. A copy of language/closure/matrix/Q_field_computed_plain_let.surql."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: NONE, id: w:1 }]"

[[test.results]]
value = "[NONE]"
*/
LET $m = 8;
DEFINE FIELD f ON w COMPUTED $m;
CREATE w:1;
SELECT VALUE f FROM w;
