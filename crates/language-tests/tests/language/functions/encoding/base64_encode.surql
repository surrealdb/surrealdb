/**
[test]

[[test.results]]
value = "''"

[[test.results]]
value = "'aGVsbG8'"

[[test.results]]
value = "''"

[[test.results]]
value = "'aGVsbG8='"

*/

encoding::base64::encode(<bytes>"");
encoding::base64::encode(<bytes>"hello", false);

encoding::base64::encode(<bytes>"", true);
encoding::base64::encode(<bytes>"hello", true);
