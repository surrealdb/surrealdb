/**
[test]
reason = "Test path specificity matching (fixed > dynamic > rest)"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: { matched: 'fixed' }, headers: {}, status: 200 }"

[[test.results]]
value = "{ body: { matched: 'dynamic' }, headers: {}, status: 200 }"

[[test.results]]
value = "{ body: { matched: 'rest' }, headers: {}, status: 200 }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: { matched: 'more_specific' }, headers: {}, status: 200 }"

[env.capabilities]
allow-experimental = ["define_api"]

*/

// Fixed path (most specific)
DEFINE API "/users/specific"
    FOR get
        THEN {
            RETURN {
                status: 200,
                body: {
                    matched: "fixed"
                }
            };
        };

// Dynamic path (less specific)
DEFINE API "/users/:id"
    FOR get
        THEN {
            RETURN {
                status: 200,
                body: {
                    matched: "dynamic"
                }
            };
        };

// Rest path (least specific)
DEFINE API "/users/*rest"
    FOR get
        THEN {
            RETURN {
                status: 200,
                body: {
                    matched: "rest"
                }
            };
        };

// Exact match should use fixed path
api::invoke("/users/specific");

// Dynamic match should use dynamic path
api::invoke("/users/123");

// Rest match should use rest path
api::invoke("/users/123/posts");

// More specific path should win
DEFINE API "/api/v1/users/:id"
    FOR get
        THEN {
            RETURN {
                status: 200,
                body: {
                    matched: "more_specific"
                }
            };
        };

DEFINE API "/api/*path"
    FOR get
        THEN {
            RETURN {
                status: 200,
                body: {
                    matched: "less_specific"
                }
            };
        };

// More specific should win
api::invoke("/api/v1/users/456");

