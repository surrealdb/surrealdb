/**
[test]
reason = "Test static path matching - exact matching required"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: { path: '/test' }, context: {  }, headers: {}, status: 200 }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: { path: '/api/users' }, context: {  }, headers: {}, status: 200 }"

[[test.results]]
value = "{ body: NONE, context: {  }, headers: {  }, status: 404 }"

[[test.results]]
value = "{ body: NONE, context: {  }, headers: {  }, status: 404 }"

[[test.results]]
value = "{ body: NONE, context: {  }, headers: {  }, status: 404 }"

[env.capabilities]
allow-experimental = ["define_api"]

*/

// Simple static path
DEFINE API "/test"
    FOR get
        THEN {
            {
                status: 200,
                body: {
                    path: "/test"
                }
            };
        };

api::invoke("/test");

// Multi-segment static path
DEFINE API "/api/users"
    FOR get
        THEN {
            {
                status: 200,
                body: {
                    path: "/api/users"
                }
            };
        };

api::invoke("/api/users");

// Path not found - should return NONE
api::invoke("/nonexistent");

// Partial match should not work
api::invoke("/test/extra");

// Different case should not match
api::invoke("/TEST");

