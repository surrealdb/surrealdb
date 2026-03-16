/**
[test]
reason = "Verify middleware execution order: DB-wide -> FOR any -> FOR method"

# 0
[[test.results]]
value = "NONE"

# 1
[[test.results]]
value = "NONE"

# 2
[[test.results]]
value = "NONE"

# 3
[[test.results]]
skip-api-request-id = true
value = "{ body: { order: ['db', 'any', 'get'] }, headers: {}, status: 200 }"

# 4
[[test.results]]
value = "NONE"

# 5
[[test.results]]
skip-api-request-id = true
value = "{ body: { order: ['db', 'any', 'post'] }, headers: {}, status: 200 }"

# 6
[[test.results]]
value = "NONE"

# 7
[[test.results]]
skip-api-request-id = true
value = "{ body: { order: ['db', 'any'] }, headers: {}, status: 200 }"

*/

# 0 - Helper function to track middleware execution order
DEFINE FUNCTION fn::track_middleware($req: object, $next: function, $name: string) {
    LET $req = $req + { body: ($req.body ?? {}) + {
        order: array::append($req.body.order ?? [], $name)
    }};
    
    RETURN $next($req);
};

# 1 - Database-level middleware
DEFINE CONFIG API MIDDLEWARE fn::track_middleware("db");

# 2 - API with FOR any and FOR get middleware
DEFINE API "/order_test"
    FOR any
        MIDDLEWARE
            fn::track_middleware("any")
        THEN {
            RETURN {
                status: 200,
                body: {
                    order: $request.body.order
                }
            };
        }
    FOR get
        MIDDLEWARE
            fn::track_middleware("get")
        THEN {
            RETURN {
                status: 200,
                body: {
                    order: $request.body.order
                }
            };
        };

# 3 - GET request should execute: db -> any -> get
api::invoke("/order_test", {
    method: "get"
});

# 4 - API with FOR any and FOR post middleware
DEFINE API "/order_test_post"
    FOR any
        MIDDLEWARE
            fn::track_middleware("any")
        THEN {
            RETURN {
                status: 200,
                body: {
                    order: $request.body.order
                }
            };
        }
    FOR post
        MIDDLEWARE
            fn::track_middleware("post")
        THEN {
            RETURN {
                status: 200,
                body: {
                    order: $request.body.order
                }
            };
        };

# 5 - POST request should execute: db -> any -> post
api::invoke("/order_test_post", {
    method: "post"
});

# 6 - API with only FOR any (no method-specific)
DEFINE API "/order_test_any_only"
    FOR any
        MIDDLEWARE
            fn::track_middleware("any")
        THEN {
            RETURN {
                status: 200,
                body: {
                    order: $request.body.order
                }
            };
        };

# 7 - Any method should execute: db -> any
api::invoke("/order_test_any_only", {
    method: "put"
});

