/**
[test]
reason = "api::req::max_body rejects oversized raw bodies before body parsing"

[[test.results]]
value = "NONE"

[[test.results]]
skip-api-request-id = true
value = "{ body: 'Invalid request body: The body exceeded the max payload size of 10b', headers: {}, status: 413 }"

[[test.results]]
skip-api-request-id = true
value = "{ body: { parsed: { a: 1 } }, headers: {}, status: 200 }"

[[test.results]]
skip-api-request-id = true
value = "{ body: 'Invalid request body: The body exceeded the max payload size of 10b', headers: {}, status: 413 }"

[[test.results]]
value = "NONE"

[[test.results]]
skip-api-request-id = true
value = "{ body: 'ok', headers: {}, status: 200 }"

[[test.results]]
value = "NONE"

[[test.results]]
skip-api-request-id = true
value = "{ body: 'ok', headers: {}, status: 200 }"

[[test.results]]
value = "NONE"

[[test.results]]
skip-api-request-id = true
value = "{ body: NONE, headers: {}, status: 500 }"

*/

-- The cap runs ahead of body parsing, so oversized payloads are never decoded
DEFINE API "/limited"
    FOR post
        MIDDLEWARE
            api::req::max_body(10),
            api::req::body("json")
        THEN {
            { status: 200, body: { parsed: $request.body } };
        };

-- Oversized bytes body is rejected with 413 before parsing
api::invoke("/limited", {
    method: "post",
    headers: { "content-type": "application/json" },
    body: <bytes>'{"data":"aaaaaaaaaaaaaaa"}'
});

-- A body under the limit is parsed and handled
api::invoke("/limited", {
    method: "post",
    headers: { "content-type": "application/json" },
    body: <bytes>'{"a":1}'
});

-- Oversized string bodies are measured by UTF-8 byte length
api::invoke("/limited", {
    method: "post",
    headers: { "content-type": "application/json" },
    body: "aaaaaaaaaaaaaaaaaaaaaaaaaa"
});

-- The limit accepts byte-size strings with units
DEFINE API "/limited_units"
    FOR post
        MIDDLEWARE
            api::req::max_body("1kb")
        THEN {
            { status: 200, body: "ok" };
        };

-- A small body passes a 1kb cap
api::invoke("/limited_units", { method: "post", body: <bytes>"small" });

-- Requests without a body are unaffected by the cap
DEFINE API "/limited_nobody"
    FOR get
        MIDDLEWARE
            api::req::max_body(1)
        THEN {
            { status: 200, body: "ok" };
        };

api::invoke("/limited_nobody");

-- An invalid limit argument surfaces at invocation as a masked error
DEFINE API "/limited_bad"
    FOR get
        MIDDLEWARE
            api::req::max_body(-1)
        THEN {
            { status: 200, body: "ok" };
        };

api::invoke("/limited_bad");
