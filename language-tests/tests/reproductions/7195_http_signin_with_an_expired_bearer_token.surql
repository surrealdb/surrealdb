/**
[test]
reason = "Transport-level: an HTTP request that carries an expired bearer token was refused by the authentication middleware before the signin and signup handlers ran. Not expressible in SurrealQL; covered by signin_and_signup_ignore_an_expired_token in tests/http_integration.rs."
issue = 7195
run = false
*/

RETURN NONE;
