/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
skip-api-request-id = true
value = "{ body: 'deadline has elapsed', headers: {  }, status: 500 }"

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
            sleep 1s;
            {}
        };

api::invoke("/exceeds_timeout");

DEFINE API "missing_slash"
    FOR get 
        THEN {
            {
                status: 200,
                headers: {}
            }
        };