/**
[test]
reason = "Test method-specific handlers and middleware"

# 0
[[test.results]]
value = "NONE"

# 1
[[test.results]]
skip-api-request-id = true
value = "{ body: { handler: 'get' }, context: {  }, headers: {}, status: 200 }"

# 2
[[test.results]]
skip-api-request-id = true
value = "{ body: { handler: 'post' }, context: {  }, headers: {}, status: 201 }"

# 3
[[test.results]]
value = "NONE"

# 4
[[test.results]]
value = "NONE"

# 5
[[test.results]]
skip-api-request-id = true
value = "{ body: { handler: 'get', middleware: 'get_mw' }, context: {  }, headers: {}, status: 200 }"

# 6
[[test.results]]
skip-api-request-id = true
value = "{ body: { handler: 'post', middleware: 'post_mw' }, context: {  }, headers: {}, status: 201 }"

[env.capabilities]
allow-experimental = ["define_api"]

*/

# 0 - Multiple methods on same route
DEFINE API "/multi_method"
    FOR get
        THEN {
            {
                status: 200,
                body: {
                    handler: "get"
                }
            };
        }
    FOR post
        THEN {
            {
                status: 201,
                body: {
                    handler: "post"
                }
            };
        };

# 1
api::invoke("/multi_method", {
    method: "get"
});

# 2
api::invoke("/multi_method", {
    method: "post"
});

# 3 - Method-specific middleware
DEFINE FUNCTION fn::method_middleware($req: object, $next: function, $name: string) -> object {
    LET $res = $next($req);
    RETURN $res + { body: $res.body + { middleware: $name } };
};

# 4
DEFINE API "/method_middleware"
    FOR get
        MIDDLEWARE
            fn::method_middleware("get_mw")
        THEN {
            {
                status: 200,
                body: {
                    handler: "get"
                }
            };
        }
    FOR post
        MIDDLEWARE
            fn::method_middleware("post_mw")
        THEN {
            {
                status: 201,
                body: {
                    handler: "post"
                }
            };
        };

# 5
api::invoke("/method_middleware", {
    method: "get"
});

# 6
api::invoke("/method_middleware", {
    method: "post"
});

