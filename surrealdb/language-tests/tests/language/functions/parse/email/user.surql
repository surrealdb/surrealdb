/**
[test]

[[test.results]]
value = "'ALLOWED'"

[[test.results]]
value = "'john.doe'"

[[test.results]]
value = "'j'"

[[test.results]]
value = "'a+b'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'\"a b\"'"

[[test.results]]
value = "'j'"

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
value = "'j.b'"

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
parse::email::user("john.doe@example.com");
parse::email::user("j@example.com");
parse::email::user("a+b@example.com");
parse::email::user("j\\@b@example.com");
parse::email::user("j\\ b@example.com");
parse::email::user('"a b"@example.com');
parse::email::user('j@[127.0.0.1]');
parse::email::user('j@[IPv6:0123:4567:89AB:CDEF:0123:4567:89AB:CDEF]');
parse::email::user('j(with a comment)@example.com');

"NOT ALLOWED";
// Not allowed email addresss
parse::email::user(".j@example.com");
parse::email::user("j.@example.com");
parse::email::user("j.b@com");
parse::email::user("j@example..com");
parse::email::user("@example.com");
parse::email::user("j\\@example.com");
parse::email::user("www.example.com");
