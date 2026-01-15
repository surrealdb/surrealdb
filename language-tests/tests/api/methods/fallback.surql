/**
[test]
reason = "Test FOR any fallback handler behavior"

# 0
[[test.results]]
value = "NONE"

# 1
[[test.results]]
value = "{ body: { handler: 'fallback' }, context: {  }, headers: {}, status: 200 }"

# 2
[[test.results]]
value = "NONE"

# 3
[[test.results]]
value = "{ body: { handler: 'get' }, context: {  }, headers: {}, status: 200 }"

# 4
[[test.results]]
value = "{ body: { handler: 'fallback' }, context: {  }, headers: {}, status: 200 }"

# 5
[[test.results]]
value = "NONE"

# 6
[[test.results]]
value = "NONE"

# 7
[[test.results]]
value = "{ body: { handler: 'fallback', middleware: 'fallback_mw' }, context: {  }, headers: {}, status: 200 }"

[env.capabilities]
allow-experimental = ["define_api"]

*/

# 0 - Fallback only (no method-specific handlers)
DEFINE API "/fallback_only"
    FOR any
        THEN {
            {
                status: 200,
                body: {
                    handler: "fallback"
                }
            };
        };

# 1
api::invoke("/fallback_only", {
    method: "get"
});

# 2 - Fallback with method-specific handler (method-specific should win)
DEFINE API "/fallback_with_get"
    FOR any
        THEN {
            {
                status: 200,
                body: {
                    handler: "fallback"
                }
            };
        }
    FOR get
        THEN {
            {
                status: 200,
                body: {
                    handler: "get"
                }
            };
        };

# 3
api::invoke("/fallback_with_get", {
    method: "get"
});

# 4 - Unmatched method should use fallback
api::invoke("/fallback_with_get", {
    method: "put"
});

# 5 - Fallback with middleware
DEFINE FUNCTION fn::fallback_middleware($req: object, $next: function) -> object {
    LET $res = $next($req);
    RETURN $res + { body: $res.body + { middleware: 'fallback_mw' } };
};

DEFINE API "/fallback_middleware"
    FOR any
        MIDDLEWARE
            fn::fallback_middleware()
        THEN {
            {
                status: 200,
                body: {
                    handler: "fallback"
                }
            };
        };

# 6
api::invoke("/fallback_middleware", {
    method: "patch"
});

