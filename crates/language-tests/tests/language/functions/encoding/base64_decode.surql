/**
[test]

[[test.results]]
value = 'b""'

[[test.results]]
value = "true"

[[test.results]]
value = "true"

*/


encoding::base64::decode("");
encoding::base64::decode("aGVsbG8") = <bytes>"hello";
encoding::base64::decode("aGVsbG8=") = <bytes>"hello";
