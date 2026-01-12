/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/
DEFINE USER user_token_duration ON DB DURATION FOR TOKEN 1h;
DEFINE USER user_session_none ON DB DURATION FOR SESSION NONE;
DEFINE USER user_session_duration ON DB DURATION FOR SESSION 1h;
