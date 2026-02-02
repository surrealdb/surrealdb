/**
[test]
reason = "Test custom user-defined middleware functions with proper signatures and behavior"

# 0
[[test.results]]
value = "NONE"

# 1
[[test.results]]
value = "NONE"

# 2
[[test.results]]
skip-api-request-id = true
value = "{ body: { modified: true, original: 'test' }, headers: { 'x-modified': 'true' }, status: 200 }"

# 3
[[test.results]]
value = "NONE"

# 4
[[test.results]]
value = "NONE"

# 5
[[test.results]]
skip-api-request-id = true
value = "{ body: { count: 2 }, headers: {}, status: 200 }"

# 6
[[test.results]]
value = "NONE"

# 7
[[test.results]]
value = "NONE"

# 8
[[test.results]]
skip-api-request-id = true
value = "{ body: { message: 'original message', prefix: 'PREFIX: original message' }, headers: {}, status: 200 }"

# 9
[[test.results]]
value = "NONE"

# 10
[[test.results]]
value = "NONE"

# 11
[[test.results]]
skip-api-request-id = true
value = "{ body: NONE, headers: {  }, status: 500 }"

[env.capabilities]
allow-experimental = ["define_api"]

*/

# 0 - Custom middleware that modifies request before next()
DEFINE FUNCTION fn::add_request_field($req: object, $next: function) -> object {
    LET $req = $req + { body: $req.body + { modified: true } };
    RETURN $next($req);
};

# 1
DEFINE API "/custom_request"
    FOR get
        MIDDLEWARE
            fn::add_request_field()
        THEN {
            {
                status: 200,
                headers: {
                    "x-modified": <string> $request.body.modified
                },
                body: $request.body
            };
        };

# 2
api::invoke("/custom_request", {
    body: {
        original: "test"
    }
});

# 3 - Custom middleware that modifies response after next()
DEFINE FUNCTION fn::increment_count($req: object, $next: function) -> object {
    LET $res = $next($req);
    LET $res = $res + { body: $res.body + { count: $res.body.count + 1 } };
    RETURN $res;
};

# 4
DEFINE API "/custom_response"
    FOR get
        MIDDLEWARE
            fn::increment_count()
        THEN {
            {
                status: 200,
                body: {
                    count: 1
                }
            };
        };

# 5
api::invoke("/custom_response");

# 6 - Custom middleware with additional parameters
DEFINE FUNCTION fn::add_prefix($req: object, $next: function, $prefix: string) -> object {
    LET $res = $next($req);
    LET $res = $res + { body: $res.body + { prefix: $prefix + ": " + $res.body.message } };
    RETURN $res;
};

# 7
DEFINE API "/custom_with_args"
    FOR get
        MIDDLEWARE
            fn::add_prefix("PREFIX")
        THEN {
            {
                status: 200,
                body: {
                    message: "original message"
                }
            };
        };

# 8
api::invoke("/custom_with_args");

# 9 - Custom middleware that throws an error
DEFINE FUNCTION fn::error_middleware($req: object, $next: function) -> object {
    THROW "Custom middleware error";
};

# 10
DEFINE API "/custom_error"
    FOR get
        MIDDLEWARE
            fn::error_middleware()
        THEN {
            {
                status: 200,
                body: {}
            };
        };

# 11
api::invoke("/custom_error");

