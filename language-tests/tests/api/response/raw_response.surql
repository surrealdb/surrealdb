/**
[test]
reason = "Test raw response mode"

# 0
[[test.results]]
value = "NONE"

# 1
[[test.results]]
match = "type::is_bytes($result.body) AND $result.status == 200"
error = false

# 2
[[test.results]]
value = "NONE"

# 3
[[test.results]]
value = "{ body: 'raw string response', context: {  }, headers: {}, status: 200 }"

# 4
[[test.results]]
value = "NONE"

# 5
[[test.results]]
match = "type::is_bytes($result.body) AND $result.headers['content-type'] == 'application/octet-stream' AND $result.status == 200"
error = false

[env.capabilities]
allow-experimental = ["define_api"]

*/

# 0 - Raw bytes response
DEFINE API "/raw/bytes"
    FOR get
        THEN {
            {
                status: 200,
                raw: true,
                body: <bytes>'raw bytes response'
            };
        };

# 1
api::invoke("/raw/bytes");

# 2 - Raw string response
DEFINE API "/raw/string"
    FOR get
        THEN {
            {
                status: 200,
                raw: true,
                body: "raw string response"
            };
        };

# 3
api::invoke("/raw/string");

# 4 - Raw response with Content-Type header
DEFINE API "/raw/with_content_type"
    FOR get
        THEN {
            {
                status: 200,
                raw: true,
                headers: {
                    "content-type": "application/octet-stream"
                },
                body: <bytes>'raw with content type'
            };
        };

# 5
api::invoke("/raw/with_content_type");

