/**
[test]
reason = "Test request body deserialization with all strategies"

# 0
[[test.results]]
value = "NONE"

# 1
[[test.results]]
value = "{ body: { parsed: { name: 'test', value: 42 } }, context: {  }, headers: {}, status: 200 }"

# 2
[[test.results]]
value = "NONE"

# 3
[[test.results]]
value = "{ body: { parsed: 'abc' }, context: {  }, headers: {}, status: 200 }"

# 4
[[test.results]]
value = "NONE"

# 5
[[test.results]]
error = "Failed to decode fb value"

# 6
[[test.results]]
value = "NONE"

# 7
[[test.results]]
value = "{ body: { parsed: 'plain text content' }, context: {  }, headers: {}, status: 200 }"

# 8
[[test.results]]
value = "NONE"

# 9
[[test.results]]
match = "type::is_bytes($result.body.parsed) AND $result.status == 200"
error = false

# 10
[[test.results]]
value = "NONE"

# 11
[[test.results]]
value = "{ body: { parsed: { native: 'format' } }, context: {  }, headers: {}, status: 200 }"

# 12
[[test.results]]
value = "NONE"

# 13
[[test.results]]
value = "{ body: { parsed: { auto: 'json' } }, context: {  }, headers: {}, status: 200 }"

# 14
[[test.results]]
value = "{ body: { parsed: b'736F6D652064617461' }, context: {  }, headers: {}, status: 200 }"

[env.capabilities]
allow-experimental = ["define_api"]

*/

# 0 - JSON strategy
DEFINE API "/body/json"
    FOR post
        MIDDLEWARE
            api::req::body("json")
        THEN {
            RETURN {
                status: 200,
                body: {
                    parsed: $request.body
                }
            };
        };

# 1
api::invoke("/body/json", {
    method: "post",
    headers: {
        "content-type": "application/json"
    },
    body: <bytes>'{"name":"test","value":42}'
});

# 2 - CBOR strategy
DEFINE API "/body/cbor"
    FOR post
        MIDDLEWARE
            api::req::body("cbor")
        THEN {
            RETURN {
                status: 200,
                body: {
                    parsed: $request.body
                }
            };
        };

# 3
api::invoke("/body/cbor", {
    method: "post",
    headers: {
        "content-type": "application/cbor"
    },
    body: encoding::cbor::encode('abc')
});

# 4 - Flatbuffers strategy (note: requires valid flatbuffers-encoded data)
# Since flatbuffers encoding is not available in SurrealQL, we test error handling
DEFINE API "/body/flatbuffers"
    FOR post
        MIDDLEWARE
            api::req::body("flatbuffers")
        THEN {
            RETURN {
                status: 200,
                body: {
                    parsed: $request.body
                }
            };
        };

# 5 - Invalid flatbuffers data should error
api::invoke("/body/flatbuffers", {
    method: "post",
    headers: {
        "content-type": "application/vnd.surrealdb.flatbuffers"
    },
    body: <bytes>'invalid_flatbuffers_data'
});

# 6 - Plain text strategy
DEFINE API "/body/plain"
    FOR post
        MIDDLEWARE
            api::req::body("plain")
        THEN {
            RETURN {
                status: 200,
                body: {
                    parsed: $request.body
                }
            };
        };

# 7
api::invoke("/body/plain", {
    method: "post",
    headers: {
        "content-type": "text/plain"
    },
    body: <bytes>'plain text content'
});

# 8 - Bytes strategy
DEFINE API "/body/bytes"
    FOR post
        MIDDLEWARE
            api::req::body("bytes")
        THEN {
            RETURN {
                status: 200,
                body: {
                    parsed: $request.body
                }
            };
        };

# 9
api::invoke("/body/bytes", {
    method: "post",
    headers: {
        "content-type": "application/octet-stream"
    },
    body: <bytes>'raw bytes'
});

# 10 - Native strategy
DEFINE API "/body/native"
    FOR post
        MIDDLEWARE
            api::req::body("native")
        THEN {
            RETURN {
                status: 200,
                body: {
                    parsed: $request.body
                }
            };
        };

# 11
api::invoke("/body/native", {
    method: "post",
    headers: {
        "content-type": "application/vnd.surrealdb.native"
    },
    body: {
        native: "format"
    }
});

# 12 - Auto strategy (Content-Type based)
DEFINE API "/body/auto"
    FOR post
        MIDDLEWARE
            api::req::body("auto")
        THEN {
            RETURN {
                status: 200,
                body: {
                    parsed: $request.body
                }
            };
        };

# 13
api::invoke("/body/auto", {
    method: "post",
    headers: {
        "content-type": "application/json"
    },
    body: <bytes>'{"auto":"json"}'
});

# 14 - Auto strategy without Content-Type (may not error, just parse as native)
api::invoke("/body/auto", {
    method: "post",
    body: <bytes>'some data'
});

