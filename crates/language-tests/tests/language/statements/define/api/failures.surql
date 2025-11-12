/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed because it exceeded the timeout"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Failed to deserialize field 'headers' on type 'ApiResponse': Expected object, got none"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Failed to deserialize field 'status' on type 'ApiResponse': Expected number, got none"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Failed to deserialize field 'status' on type 'ApiResponse': Expected number, got none"

[[test.results]]
error = "The string could not be parsed into a path: Segment should start with /"

[env.capabilities]
allow-experimental = ["define_api"]

*/

DEFINE API "/exceeds_timeout"
    FOR get 
        MIDDLEWARE
            api::timeout(1ns)
        THEN {
            CREATE something;
            RETURN {
                status: 200,
                headers: {},
                body: {}
            };
        };

api::invoke("exceeds_timeout");

DEFINE API "/no_headers"
    FOR get 
        THEN {
            RETURN {
                status: 200
            };
        };

api::invoke("no_headers");

DEFINE API "/no_status"
    FOR get 
        THEN {
            RETURN {
                headers: {}
            };
        };

api::invoke("no_status");


DEFINE API "/nothing_at_all"
    FOR get 
        THEN {};

api::invoke("nothing_at_all");

DEFINE API "missing_slash"
    FOR get 
        THEN {
            RETURN {
                status: 200,
                headers: {}
            }
        };