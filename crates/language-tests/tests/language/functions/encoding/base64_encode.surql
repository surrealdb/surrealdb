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
encoding::base64::encode(<bytes>"hello");

encoding::base64::encode_padded(<bytes>"");
encoding::base64::encode_padded(<bytes>"hello");
