/**
[test]
reason = "Test HTTP status code handling"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: {}, context: {  }, headers: {}, status: 200 }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: {}, context: {  }, headers: {}, status: 201 }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: {}, context: {  }, headers: {}, status: 400 }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: {}, context: {  }, headers: {}, status: 404 }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: {}, context: {  }, headers: {}, status: 500 }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: { original: 200 }, context: {  }, headers: {}, status: 404 }"

[env.capabilities]
allow-experimental = ["define_api"]

*/

// Status code in RETURN object
DEFINE API "/status/200"
    FOR get
        THEN {
            {
                status: 200,
                body: {}
            };
        };

api::invoke("/status/200");

DEFINE API "/status/201"
    FOR get
        THEN {
            {
                status: 201,
                body: {}
            };
        };

api::invoke("/status/201");

DEFINE API "/status/400"
    FOR get
        THEN {
            {
                status: 400,
                body: {}
            };
        };

api::invoke("/status/400");

DEFINE API "/status/404"
    FOR get
        THEN {
            {
                status: 404,
                body: {}
            };
        };

api::invoke("/status/404");

DEFINE API "/status/500"
    FOR get
        THEN {
            {
                status: 500,
                body: {}
            };
        };

api::invoke("/status/500");

// Status code via middleware (overrides RETURN)
DEFINE API "/status/middleware"
    FOR get
        MIDDLEWARE
            api::res::status(404)
        THEN {
            {
                status: 200,
                body: {
                    original: 200
                }
            };
        };

api::invoke("/status/middleware");

