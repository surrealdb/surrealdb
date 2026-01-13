/**
[test]
reason = "Test middleware error scenarios: function not found"

# 0
[[test.results]]
value = "NONE"

# 1
[[test.results]]
error = "The function 'fn::nonexistent' does not exist"

[env.capabilities]
allow-experimental = ["define_api"]

*/

# 0 - Middleware function that doesn't exist (error during DEFINE)
DEFINE API "/middleware/nonexistent"
    FOR get
        MIDDLEWARE
            fn::nonexistent()
        THEN {
            RETURN {
                status: 200,
                body: {}
            };
        };

# 1 - The DEFINE itself fails, so this won't execute
api::invoke("/middleware/nonexistent");
