/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: {  }, headers: { \"requested-at\": 'October 22' }, raw: NONE, status: 404 }"

[[test.results]]
value = "{ body: {  }, headers: { \"requested-at\": 'October 22' }, raw: NONE, status: 404 }"

[[test.results]]
error = "An API error occurred: Invalid request body: Expected data frame but received another frame type"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: { some: 'info' }, headers: { \"country-origin\": 'CA', \"requested-at\": 'October 22' }, raw: NONE, status: 200 }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: { some: 'info' }, headers: { \"country-origin\": 'CA', language: 'FR', \"requested-at\": 'October 22' }, raw: NONE, status: 200 }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: {  }, headers: { age: '10', \"requested-at\": 'October 22' }, raw: NONE, status: 404 }"

[[test.results]]
value = "{ body: {  }, headers: { age: '10', \"requested-at\": 'October 22' }, raw: NONE, status: 404 }"

[[test.results]]
error = "An API error occurred: Invalid request body: Expected data frame but received another frame type"

[[test.results]]
value = "{ body: { some: 'info' }, headers: { age: '10', \"country-origin\": 'CA', \"requested-at\": 'October 22' }, raw: NONE, status: 200 }"

[[test.results]]
value = "{ body: { some: 'info' }, headers: { age: '10', \"country-origin\": 'CA', language: 'FR', \"requested-at\": 'October 22' }, raw: NONE, status: 200 }"

[env.capabilities]
allow-experimental = ["define_api"]

*/

// api::req::raw_body
DEFINE API "/raw_body"
    FOR get 
        MIDDLEWARE
            api::req::raw_body(true)
        THEN {
            RETURN {
                status: 404,
                headers: {
                    "requested-at": "October 22"
                },
                body: {}
            };
        };

// Bytes work
api::invoke("/raw_body", {
    body: <bytes>"Hi plz send the information"
});

// Strings convert to bytes, also fine
api::invoke("/raw_body", {
    body: "Hi plz send the information"
});

// Anything else returns an error
api::invoke("/raw_body", {
    body: {
        one: "two"
    }
});


// api::res::header
DEFINE API "/canada"
    FOR get 
        MIDDLEWARE
            api::res::header("country-origin", "CA")
        THEN {
            RETURN {
                status: 200,
                headers: {
                    "requested-at": "October 22"
                },
                body: {
                    some: "info"
                }
            };
        };

api::invoke("canada");


// api:res::headers
DEFINE API "/canada2"
    FOR get 
        MIDDLEWARE
            api::res::headers({
                "country-origin": "CA",
                "language": "FR"
            })
        THEN {
            RETURN {
                status: 200,
                headers: {
                    "requested-at": "October 22"
                },
                body: {
                    some: "info"
                }
            };
        };

api::invoke("canada2");

DEFINE CONFIG API MIDDLEWARE api::res::header("Age", "10");

api::invoke("/raw_body", {
    body: <bytes>"Hi plz send the information"
});

// Strings convert to bytes, also fine
api::invoke("/raw_body", {
    body: "Hi plz send the information"
});

// Anything else returns an error
api::invoke("/raw_body", {
    body: {
        one: "two"
    }
});

api::invoke("canada");

api::invoke("canada2");