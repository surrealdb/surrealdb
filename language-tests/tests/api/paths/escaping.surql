/**
[test]
reason = "Test path escaping with \\: and \\* to use literal colon and asterisk"

# 0
[[test.results]]
value = "NONE"

# 1
[[test.results]]
skip-api-request-id = true
value = "{ body: { path: '/test/:colon' }, headers: {}, status: 200 }"

# 2
[[test.results]]
value = "NONE"

# 3
[[test.results]]
skip-api-request-id = true
value = "{ body: { path: '/star/*star' }, headers: {}, status: 200 }"

# 4
[[test.results]]
value = "NONE"

# 5
[[test.results]]
skip-api-request-id = true
value = "{ body: { path: '/both/:colon/*star' }, headers: {}, status: 200 }"

[env.capabilities]
allow-experimental = ["define_api"]

*/

# 0 - Escaped colon in path
DEFINE API "/test/\\:colon"
    FOR get
        THEN {
            {
                status: 200,
                body: {
                    path: "/test/:colon"
                }
            };
        };

# 1
api::invoke("/test/:colon");

# 2 - Escaped asterisk in path (note: *star might match as rest segment, so we use a different base path)
DEFINE API "/star/\\*star"
    FOR get
        THEN {
            {
                status: 200,
                body: {
                    path: "/star/*star"
                }
            };
        };

# 3
api::invoke("/star/*star");

# 4 - Both escaped characters (using different base to avoid conflicts)
DEFINE API "/both/\\:colon/\\*star"
    FOR get
        THEN {
            {
                status: 200,
                body: {
                    path: "/both/:colon/*star"
                }
            };
        };

# 5
api::invoke("/both/:colon/*star");

