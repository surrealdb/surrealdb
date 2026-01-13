/**
[test]
reason = "Test response body serialization with all strategies"

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
match = "type::is_bytes($result.body) AND $result.headers['content-type'] == 'application/cbor' AND $result.status == 200"
error = false

# 4
[[test.results]]
value = "NONE"

# 5
[[test.results]]
match = "type::is_bytes($result.body) AND $result.headers['content-type'] == 'application/vnd.surrealdb.flatbuffers' AND $result.status == 200"
error = false

# 6
[[test.results]]
value = "NONE"

# 7
[[test.results]]
match = "type::is_bytes($result.body) AND $result.headers['content-type'] == 'text/plain' AND $result.status == 200"
error = false

# 8
[[test.results]]
value = "NONE"

# 9
[[test.results]]
match = "type::is_bytes($result.body) AND $result.headers['content-type'] == 'application/octet-stream' AND $result.status == 200"
error = false

# 10
[[test.results]]
value = "NONE"

# 11
[[test.results]]
value = "{ body: { native: 'format' }, headers: { 'content-type': 'application/vnd.surrealdb.native' }, status: 200 }"

[env.capabilities]
allow-experimental = ["define_api"]

*/

# 0 - JSON strategy
DEFINE API "/res/json"
    FOR get
        MIDDLEWARE
            api::res::body("json")
        THEN {
            RETURN {
                status: 200,
                body: {
                    message: "Hello"
                }
            };
        };

# 1
api::invoke("/res/json");

# 2 - CBOR strategy
DEFINE API "/res/cbor"
    FOR get
        MIDDLEWARE
            api::res::body("cbor")
        THEN {
            RETURN {
                status: 200,
                body: {
                    data: "test"
                }
            };
        };

# 3
api::invoke("/res/cbor");

# 4 - Flatbuffers strategy
DEFINE API "/res/flatbuffers"
    FOR get
        MIDDLEWARE
            api::res::body("flatbuffers")
        THEN {
            RETURN {
                status: 200,
                body: {
                    data: "test"
                }
            };
        };

# 5
api::invoke("/res/flatbuffers");

# 6 - Plain text strategy
DEFINE API "/res/plain"
    FOR get
        MIDDLEWARE
            api::res::body("plain")
        THEN {
            RETURN {
                status: 200,
                body: "plain text"
            };
        };

# 7
api::invoke("/res/plain");

# 8 - Bytes strategy
DEFINE API "/res/bytes"
    FOR get
        MIDDLEWARE
            api::res::body("bytes")
        THEN {
            RETURN {
                status: 200,
                body: <bytes>'raw bytes'
            };
        };

# 9
api::invoke("/res/bytes");

# 10 - Native strategy
DEFINE API "/res/native"
    FOR get
        MIDDLEWARE
            api::res::body("native")
        THEN {
            RETURN {
                status: 200,
                body: {
                    native: "format"
                }
            };
        };

# 11
api::invoke("/res/native");

