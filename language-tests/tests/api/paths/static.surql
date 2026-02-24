/**
[test]
reason = "Test static path matching - exact matching required"

[[test.results]]
value = "NONE"

[[test.results]]
skip-api-request-id = true
value = "{ body: { path: '/test' }, headers: {}, status: 200 }"

[[test.results]]
value = "NONE"

[[test.results]]
skip-api-request-id = true
value = "{ body: { path: '/api/users' }, headers: {}, status: 200 }"

[[test.results]]
skip-api-request-id = true
value = "{ body: 'Not found', headers: {  }, status: 404 }"

[[test.results]]
skip-api-request-id = true
value = "{ body: 'Not found', headers: {  }, status: 404 }"

[[test.results]]
skip-api-request-id = true
value = "{ body: 'Not found', headers: {  }, status: 404 }"

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

