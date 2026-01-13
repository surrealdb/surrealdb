/**
[test]
reason = "Test Accept header negotiation and quality factors"

# 0
[[test.results]]
value = "NONE"

# 1
[[test.results]]
match = "type::is_bytes($result.body) AND $result.headers['content-type'] == 'application/json' AND $result.status == 200"
error = false

# 2
[[test.results]]
value = "NONE"

# 3
[[test.results]]
match = "type::is_bytes($result.body) AND $result.headers['content-type'] == 'text/plain' AND $result.status == 200"
error = false

# 4
[[test.results]]
value = "NONE"

# 5
[[test.results]]
match = "type::is_bytes($result.body) AND $result.headers['content-type'] == 'application/json' AND $result.status == 200"
error = false

# 6
[[test.results]]
match = "type::is_bytes($result.body) AND $result.headers['content-type'] == 'text/plain' AND $result.status == 200"
error = false

# 7
[[test.results]]
error = "No output strategy was possible for this API request"

[env.capabilities]
allow-experimental = ["define_api"]

*/

# 0 - Auto strategy with Accept header - JSON preferred
DEFINE API "/negotiate/auto"
    FOR get
        MIDDLEWARE
            api::res::body("auto")
        THEN {
            RETURN {
                status: 200,
                body: {
                    data: "json"
                }
            };
        };

# 1
api::invoke("/negotiate/auto", {
    headers: {
        "accept": "application/json"
    }
});

# 2 - Auto strategy with Accept header - Plain text preferred
DEFINE API "/negotiate/plain"
    FOR get
        MIDDLEWARE
            api::res::body("auto")
        THEN {
            RETURN {
                status: 200,
                body: "plain text"
            };
        };

# 3
api::invoke("/negotiate/plain", {
    headers: {
        "accept": "text/plain"
    }
});

# 4 - Quality factor - JSON with higher q should win
DEFINE API "/negotiate/q"
    FOR get
        MIDDLEWARE
            api::res::body("auto")
        THEN {
            RETURN {
                status: 200,
                body: {
                    data: "json"
                }
            };
        };

# 5
api::invoke("/negotiate/q", {
    headers: {
        "accept": "text/plain;q=0.5, application/json;q=0.9"
    }
});

# 6 - Multiple Accept values - first match wins
api::invoke("/negotiate/q", {
    headers: {
        "accept": "text/plain, application/json"
    }
});

# 7 - No match - should error
api::invoke("/negotiate/q", {
    headers: {
        "accept": "application/x-unsupported"
    }
});

