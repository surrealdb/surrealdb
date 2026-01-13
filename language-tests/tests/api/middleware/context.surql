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
DEFINE FUNCTION fn::set_request_context($req: object, $next: function) {
    LET $context = IF type::is_object($req.context) THEN $req.context ELSE {} END;
    LET $context = SELECT *, 'set' AS middleware1 FROM ONLY $context;
    LET $req = SELECT *, $context AS context FROM ONLY $req;
    RETURN $next($req);
};

# 1 - Middleware that reads response context and adds to it
DEFINE FUNCTION fn::read_response_context($req: object, $next: function) {
    LET $res = $next($req);
    LET $responseContext = IF type::is_object($res.context) THEN $res.context ELSE {} END;
    LET $responseContext = SELECT *, 'read' AS middleware4 FROM ONLY $responseContext;
    LET $res = SELECT *, $responseContext AS context FROM ONLY $res;
    RETURN $res;
};

# 2 - Test: Request and response context
DEFINE API "/context/test"
    FOR get
        MIDDLEWARE
            fn::set_request_context(),
            fn::read_response_context()
        THEN {
            LET $requestContext = IF type::is_object($request.context) THEN $request.context ELSE {} END;
            LET $body = {
                message: "Hello",
                requestContext: $requestContext
            };
            RETURN {
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
DEFINE FUNCTION fn::add_to_request_context($req: object, $next: function) {
    LET $context = IF type::is_object($req.context) THEN $req.context ELSE {} END;
    LET $context = SELECT *, 'added' AS middleware1 FROM ONLY $context;
    LET $req = SELECT *, $context AS context FROM ONLY $req;
    RETURN $next($req);
};

DEFINE FUNCTION fn::read_response_and_add_to_body($req: object, $next: function) {
    LET $res = $next($req);
    LET $requestContext = IF type::is_object($req.context) THEN $req.context ELSE {} END;
    LET $responseContext = IF type::is_object($res.context) THEN $res.context ELSE {} END;
    LET $body = IF type::is_object($res.body) THEN $res.body ELSE {} END;
    LET $body = SELECT *, $requestContext AS requestContext, $responseContext AS responseContext FROM ONLY $body;
    LET $res = SELECT *, $body AS body FROM ONLY $res;
    RETURN $res;
};

DEFINE API "/context/initial"
    FOR get
        MIDDLEWARE
            fn::add_to_request_context(),
            fn::read_response_and_add_to_body()
        THEN {
            RETURN {
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
DEFINE FUNCTION fn::context_step1($req: object, $next: function) {
    LET $context = IF type::is_object($req.context) THEN $req.context ELSE {} END;
    LET $context = SELECT *, 'value1' AS step1 FROM ONLY $context;
    LET $req = SELECT *, $context AS context FROM ONLY $req;
    RETURN $next($req);
};

DEFINE FUNCTION fn::context_step2($req: object, $next: function) {
    LET $context = IF type::is_object($req.context) THEN $req.context ELSE {} END;
    LET $context = SELECT *, 'value2' AS step2 FROM ONLY $context;
    LET $req = SELECT *, $context AS context FROM ONLY $req;
    LET $res = $next($req);
    LET $resContext = IF type::is_object($res.context) THEN $res.context ELSE {} END;
    LET $resContext = SELECT *, 'value2' AS step2 FROM ONLY $resContext;
    LET $res = SELECT *, $resContext AS context FROM ONLY $res;
    RETURN $res;
};

DEFINE FUNCTION fn::context_step3($req: object, $next: function) {
    LET $context = IF type::is_object($req.context) THEN $req.context ELSE {} END;
    LET $context = SELECT *, 'value3' AS step3 FROM ONLY $context;
    LET $req = SELECT *, $context AS context FROM ONLY $req;
    LET $res = $next($req);
    LET $resContext = IF type::is_object($res.context) THEN $res.context ELSE {} END;
    LET $resContext = SELECT *, 'value3' AS step3 FROM ONLY $resContext;
    LET $res = SELECT *, $resContext AS context FROM ONLY $res;
    RETURN $res;
};

DEFINE FUNCTION fn::context_final($req: object, $next: function) {
    LET $res = $next($req);
    LET $requestContext = IF type::is_object($req.context) THEN $req.context ELSE {} END;
    LET $responseContext = IF type::is_object($res.context) THEN $res.context ELSE {} END;
    LET $body = IF type::is_object($res.body) THEN $res.body ELSE {} END;
    LET $body = SELECT *, $requestContext AS requestContext, $responseContext AS responseContext FROM ONLY $body;
    LET $res = SELECT *, $body AS body FROM ONLY $res;
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
            RETURN {
                status: 200,
                body: {
                    message: "Chain"
                }
            };
        };

# 8
api::invoke("/context/chain");
