/**
[test]
reason = "Test multiple middleware at the same level (DB, route, method)"

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
value = "NONE"

# 6
[[test.results]]
value = "NONE"

# 7
[[test.results]]
value = "NONE"

# 8
[[test.results]]
value = "{ body: { db1: true, db2: true }, context: {  }, headers: {}, status: 200 }"

# 9
[[test.results]]
value = "NONE"

# 10
[[test.results]]
value = "{ body: { db1: true, db2: true, route1: true, route2: true }, context: {  }, headers: {}, status: 200 }"

# 11
[[test.results]]
value = "NONE"

# 12
[[test.results]]
value = "{ body: { db1: true, db2: true, method1: true, method2: true }, context: {  }, headers: {}, status: 200 }"

[env.capabilities]
allow-experimental = ["define_api"]

*/

# 0 - Helper functions for multiple middleware
DEFINE FUNCTION fn::db_middleware1($req: object, $next: function) -> object {
    LET $res = $next($req);
    LET $res = SELECT *, (SELECT *, true AS db1 FROM ONLY $res.body) AS body FROM ONLY $res;
    RETURN $res;
};

# 1
DEFINE FUNCTION fn::db_middleware2($req: object, $next: function) -> object {
    LET $res = $next($req);
    LET $res = SELECT *, (SELECT *, true AS db2 FROM ONLY $res.body) AS body FROM ONLY $res;
    RETURN $res;
};

# 2
DEFINE FUNCTION fn::route_middleware1($req: object, $next: function) -> object {
    LET $res = $next($req);
    LET $res = SELECT *, (SELECT *, true AS route1 FROM ONLY $res.body) AS body FROM ONLY $res;
    RETURN $res;
};

# 3
DEFINE FUNCTION fn::route_middleware2($req: object, $next: function) -> object {
    LET $res = $next($req);
    LET $res = SELECT *, (SELECT *, true AS route2 FROM ONLY $res.body) AS body FROM ONLY $res;
    RETURN $res;
};

# 4
DEFINE FUNCTION fn::method_middleware1($req: object, $next: function) -> object {
    LET $res = $next($req);
    LET $res = SELECT *, (SELECT *, true AS method1 FROM ONLY $res.body) AS body FROM ONLY $res;
    RETURN $res;
};

# 5
DEFINE FUNCTION fn::method_middleware2($req: object, $next: function) -> object {
    LET $res = $next($req);
    LET $res = SELECT *, (SELECT *, true AS method2 FROM ONLY $res.body) AS body FROM ONLY $res;
    RETURN $res;
};

# 6 - Multiple DB-level middleware
DEFINE CONFIG API MIDDLEWARE fn::db_middleware1(), fn::db_middleware2();

DEFINE API "/multiple/db"
    FOR get
        THEN {
            {
                status: 200,
                body: {}
            };
        };

# 9
api::invoke("/multiple/db");

# 10 - Multiple route-level middleware
DEFINE API "/multiple/route"
    FOR any
        MIDDLEWARE
            fn::route_middleware1(),
            fn::route_middleware2()
        THEN {
            {
                status: 200,
                body: {}
            };
        };

# 11
api::invoke("/multiple/route");

# 12 - Multiple method-level middleware
DEFINE API "/multiple/method"
    FOR get
        MIDDLEWARE
            fn::method_middleware1(),
            fn::method_middleware2()
        THEN {
            {
                status: 200,
                body: {}
            };
        };

# 13
api::invoke("/multiple/method");

