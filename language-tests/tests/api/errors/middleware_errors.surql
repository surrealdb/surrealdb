/**
[test]
reason = "Test middleware error scenarios: function not found"

# 0
[[test.results]]
value = "NONE"

# 1
[[test.results]]
skip-api-request-id = true
value = "{ body: NONE, context: {  }, headers: {  }, status: 500 }"

[env.capabilities]
allow-experimental = ["define_api"]

*/

# 0 - Middleware function that doesn't exist (error during DEFINE)
DEFINE API "/middleware/nonexistent"
    FOR get
        MIDDLEWARE
            fn::nonexistent()
        THEN {
            {
                status: 200,
                body: {}
            };
        };

# 1 - The DEFINE itself fails, so this won't execute
api::invoke("/middleware/nonexistent");
