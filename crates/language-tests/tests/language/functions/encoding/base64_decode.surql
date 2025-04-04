/**
[test]

[[test.results]]
value = 'encoding::base64::decode("")'

[[test.results]]
value = "true"

[[test.results]]
value = "true"

*/


encoding::base64::decode("");
encoding::base64::decode("aGVsbG8") = <bytes>"hello";
encoding::base64::decode("aGVsbG8=") = <bytes>"hello";
