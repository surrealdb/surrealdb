/**
[test]
reason = "Test missing handler scenarios"

# 0
[[test.results]]
value = "NONE"

# 1
[[test.results]]
skip-api-request-id = true
value = "{ body: 'Not found', context: {  }, headers: {  }, status: 404 }"

# 2
[[test.results]]
skip-api-request-id = true
value = "{ body: { handler: 'get' }, context: {  }, headers: {}, status: 200 }"

# 3
[[test.results]]
value = "NONE"

# 4
[[test.results]]
skip-api-request-id = true
value = "{ body: 'Not found', context: {  }, headers: {  }, status: 404 }"

# 5
[[test.results]]
value = "NONE"

# 6
[[test.results]]
skip-api-request-id = true
value = "{ body: NONE, context: {  }, headers: {  }, status: 500 }"

[env.capabilities]
allow-experimental = ["define_api"]

*/

# 0 - No handler for method (should return NONE)
DEFINE API "/no_handler"
    FOR get
        THEN {
            {
                status: 200,
                body: {
                    handler: "get"
                }
            };
        };

# 1 - POST has no handler - should return NONE  
api::invoke("/no_handler", {
    method: "post"
});

# 2 - GET works
api::invoke("/no_handler", {
    method: "get"
});

# 3 - No fallback handler - should return NONE
DEFINE API "/no_fallback"
    FOR get
        THEN {
            {
                status: 200,
                body: {}
            };
        };

# 4
api::invoke("/no_fallback", {
    method: "put"
});

# 5 - Invalid middleware function name (error occurs during DEFINE, not invoke)
DEFINE API "/invalid_middleware"
    FOR get
        MIDDLEWARE
            fn::nonexistent()
        THEN {
            {
                status: 200,
                body: {}
            };
        };

# 6
api::invoke("/invalid_middleware");

