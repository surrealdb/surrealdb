/**
[test]
reason = "Test context field for sharing metadata between middleware without touching body"

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
value = "{ body: { message: 'Hello', requestContext: { middleware1: 'set' } }, context: { middleware4: 'read' }, headers: {  }, status: 200 }"

# 4
[[test.results]]
value = "NONE"

# 5
[[test.results]]
value = "NONE"

# 6
[[test.results]]
value = "NONE"

# 7
[[test.results]]
value = "{ body: { message: 'Test', requestContext: { initial: 'value', middleware1: 'added' }, responseContext: { handler: 'set', middleware2: 'read' } }, context: { handler: 'set', middleware2: 'read' }, headers: {  }, status: 200 }"

# 8
[[test.results]]
value = "NONE"

# 9
[[test.results]]
value = "NONE"

# 10
[[test.results]]
value = "NONE"

# 11
[[test.results]]
value = "NONE"

# 12
[[test.results]]
value = "NONE"

# 13
[[test.results]]
value = "{ body: { message: 'Chain', requestContext: { step1: 'value1', step2: 'value2', step3: 'value3' }, responseContext: {  } }, context: { step2: 'value2', step3: 'value3' }, headers: {  }, status: 200 }"

[env.capabilities]
allow-experimental = ["define_api"]

*/

# 0 - Middleware that sets request context
DEFINE FUNCTION fn::set_request_context($req: object, $next: function) -> object {
    LET $req = $req + { context: $req.context + { middleware1: 'set' } };
    RETURN $next($req);
};

# 1 - Middleware that reads response context and adds to it
DEFINE FUNCTION fn::read_response_context($req: object, $next: function) -> object {
    LET $res = $next($req);
    RETURN $res + { context: $res.context + { middleware4: 'read' } };
};

# 2 - Test: Request and response context
DEFINE API "/context/test"
    FOR get
        MIDDLEWARE
            fn::set_request_context(),
            fn::read_response_context()
        THEN {
            LET $body = {
                message: "Hello",
                requestContext: $request.context ?? {},
            };

            {
                status: 200,
                body: $body,
                context: {
                    middleware4: "read"
                }
            };
        };

# 2
api::invoke("/context/test");

# 3 - Test: Initial context from request
DEFINE FUNCTION fn::add_to_request_context($req: object, $next: function) -> object {
    LET $req = $req + { context: $req.context + { middleware1: 'added' } };
    RETURN $next($req);
};

DEFINE FUNCTION fn::read_response_and_add_to_body($req: object, $next: function) -> object {
    LET $res = $next($req);
    LET $res = $res + { body: $res.body + {
        requestContext: $req.context ?? {},
        responseContext: $res.context ?? {},
    } };

    RETURN $res;
};

DEFINE API "/context/initial"
    FOR get
        MIDDLEWARE
            fn::add_to_request_context(),
            fn::read_response_and_add_to_body()
        THEN {
            {
                status: 200,
                body: {
                    message: "Test"
                },
                context: {
                    handler: "set",
                    middleware2: "read"
                }
            };
        };

# 4
api::invoke("/context/initial", {
    context: {
        initial: "value"
    }
});

# 5 - Test: Context chaining through multiple middleware
DEFINE FUNCTION fn::context_step1($req: object, $next: function) -> object {
    LET $req = $req + { context: $req.context + { step1: 'value1' } };
    RETURN $next($req);
};

DEFINE FUNCTION fn::context_step2($req: object, $next: function) -> object {
    LET $req = $req + { context: $req.context + { step2: 'value2' } };

    LET $res = $next($req);
    RETURN $res + { context: $res.context + { step2: 'value2' } };
};

DEFINE FUNCTION fn::context_step3($req: object, $next: function) -> object {
    LET $req = $req + { context: $req.context + { step3: 'value3' } };

    LET $res = $next($req);
    RETURN $res + { context: $res.context + { step3: 'value3' } };
};

DEFINE FUNCTION fn::context_final($req: object, $next: function) -> object {
    LET $res = $next($req);
    LET $res = $res + { body: $res.body + {
        requestContext: $req.context ?? {},
        responseContext: $res.context ?? {},
    } };
    
    RETURN $res;
};

DEFINE API "/context/chain"
    FOR get
        MIDDLEWARE
            fn::context_step1(),
            fn::context_step2(),
            fn::context_step3(),
            fn::context_final()
        THEN {
            {
                status: 200,
                body: {
                    message: "Chain"
                }
            };
        };

# 8
api::invoke("/context/chain");
