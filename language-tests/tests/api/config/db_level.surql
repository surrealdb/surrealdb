/**
[test]
reason = "Test database-level API configuration"

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
value = "NONE"

# 4
[[test.results]]
value = "NONE"

# 5
[[test.results]]
value = "{ body: { db_middleware: true }, context: {  }, headers: {}, status: 200 }"

# 6
[[test.results]]
value = "NONE"

# 7
[[test.results]]
value = "{ body: { db_middleware: true, route_middleware: true }, context: {  }, headers: {}, status: 200 }"

# 8
[[test.results]]
value = "NONE"

# 9
[[test.results]]
value = "{ body: { db_middleware: true, method_middleware: true }, context: {  }, headers: {}, status: 200 }"

[env.capabilities]
allow-experimental = ["define_api"]

*/

# 0 - Helper function to track middleware
DEFINE FUNCTION fn::db_middleware($req: object, $next: function) -> object {
    LET $res = $next($req);
    RETURN $res + { body: $res.body + { db_middleware: true } };
};

# 1
DEFINE FUNCTION fn::route_middleware($req: object, $next: function) -> object {
    LET $res = $next($req);
    RETURN $res + { body: $res.body + { route_middleware: true } };
};

# 2
DEFINE FUNCTION fn::method_middleware($req: object, $next: function) -> object {
    LET $res = $next($req);
    RETURN $res + { body: $res.body + { method_middleware: true } };
};

# 3 - Database-level middleware
DEFINE CONFIG API MIDDLEWARE fn::db_middleware();

# 4 - API without route or method middleware
DEFINE API "/db_only"
    FOR get
        THEN {
            {
                status: 200,
                body: {}
            };
        };

# 5
api::invoke("/db_only");

# 6 - API with route-level middleware (should combine with DB-level)
DEFINE API "/db_and_route"
    FOR any
        MIDDLEWARE
            fn::route_middleware()
        THEN {
            {
                status: 200,
                body: {}
            };
        };

# 7
api::invoke("/db_and_route");

# 8 - API with method-level middleware (should combine with DB-level)
DEFINE API "/db_and_method"
    FOR get
        MIDDLEWARE
            fn::method_middleware()
        THEN {
            {
                status: 200,
                body: {}
            };
        };

# 9
api::invoke("/db_and_method");

