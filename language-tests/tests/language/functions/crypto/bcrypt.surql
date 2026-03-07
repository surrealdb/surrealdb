/**
[env]
timeout = 30000
context-timeout = 15000

[test]
reason = "Test crypto::bcrypt::generate() and crypto::bcrypt::compare()"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

*/
LET $hash = crypto::bcrypt::generate("password123");
crypto::bcrypt::compare($hash, "password123");
crypto::bcrypt::compare($hash, "wrong_password");
