/**
[test]

[[test.results]]
value = "'ALLOWED'"

[[test.results]]
value = "'example.com'"

[[test.results]]
value = "'example.com'"

[[test.results]]
value = "'example.com'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'example.com'"

[[test.results]]
value = "'127.0.0.1'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'NOT ALLOWED'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'com'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/


"ALLOWED";
// All emails below are allowed according to RFC2822.
parse::email::host("john.doe@example.com");
parse::email::host("j@example.com");
parse::email::host("a+b@example.com");
parse::email::host("j\\@b@example.com");
parse::email::host("j\\ b@example.com");
parse::email::host('"a b"@example.com');
parse::email::host('j@[127.0.0.1]');
parse::email::host('j@[IPv6:0123:4567:89AB:CDEF:0123:4567:89AB:CDEF]');
parse::email::host('j(with a comment)@example.com');

"NOT ALLOWED";
// Not allowed email addresss
parse::email::host(".j@example.com");
parse::email::host("j.@example.com");
parse::email::host("j.b@com");
parse::email::host("j@example..com");
parse::email::host("@example.com");
parse::email::host("j\\@example.com");
parse::email::host("www.example.com");
