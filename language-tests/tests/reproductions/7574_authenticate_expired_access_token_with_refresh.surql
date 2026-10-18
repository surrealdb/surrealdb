/**
[test]
reason = "Embedded SDK only: authenticate with an expired access token and a refresh token deadlocked the session lock. Already fixed on this tree; covered by authenticate_refreshes_an_expired_access_token in surrealdb/tests/api_integration/mod.rs."
issue = 7574
run = false
*/

RETURN NONE;
