/**
[test]
reason = "Test rest path segments with *param syntax"

# 0
[[test.results]]
value = "NONE"

# 1
[[test.results]]
value = "{ body: { rest: ['segment1'] }, context: {  }, headers: {}, status: 200 }"

# 2
[[test.results]]
value = "NONE"

# 3
[[test.results]]
value = "{ body: { rest: ['v1', 'users', '123'] }, context: {  }, headers: {}, status: 200 }"

# 4
[[test.results]]
value = "NONE"

# 5
[[test.results]]
value = "{ body: NONE, context: {  }, headers: {  }, status: 404 }"

# 6
[[test.results]]
value = "NONE"

# 7
[[test.results]]
value = "{ body: { path: ['a', 'b', 'c'] }, context: {  }, headers: {}, status: 200 }"

[env.capabilities]
allow-experimental = ["define_api"]

*/

# 0 - Rest segment at end
DEFINE API "/test/*rest"
    FOR get
        THEN {
            {
                status: 200,
                body: {
                    rest: $request.params.rest
                }
            };
        };

# 1
api::invoke("/test/segment1");

# 2 - Multiple segments captured (rest captures after /api, not including it)
DEFINE API "/api/*path"
    FOR get
        THEN {
            {
                status: 200,
                body: {
                    rest: $request.params.path
                }
            };
        };

# 3
api::invoke("/api/v1/users/123");

# 4 - No segments after rest (doesn't match - rest requires at least one segment)
DEFINE API "/empty/*rest"
    FOR get
        THEN {
            {
                status: 200,
                body: {
                    rest: $request.params.rest
                }
            };
        };

# 5
api::invoke("/empty");

# 6 - Rest segment with custom name
DEFINE API "/custom/*path"
    FOR get
        THEN {
            {
                status: 200,
                body: {
                    path: $request.params.path
                }
            };
        };

# 7
api::invoke("/custom/a/b/c");

