/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''{ "/test": "DEFINE API '/test' FOR any MIDDLEWARE api::req::max_body('3mb') PERMISSIONS FULL THEN { RETURN { status: 200, body: 'OK' } } FOR get, post MIDDLEWARE api::req::max_body('4mb') PERMISSIONS FULL THEN { RETURN { status: 200, body: 'OK' } } COMMENT 'test'" }'''

[[test.results]]
value = '''{ API: "API MIDDLEWARE api::req::max_body('1mb') PERMISSIONS FULL" }'''

*/

DEFINE CONFIG API
    MIDDLEWARE api::req::max_body("1mb") 
    PERMISSIONS FULL;

DEFINE API "/test"
    FOR any 
        MIDDLEWARE api::req::max_body("3mb") 
        PERMISSIONS FULL
        THEN {
            RETURN {
                status: 200,
                body: "OK",
            }
        }
    FOR get, post 
        MIDDLEWARE api::req::max_body("4mb") 
        PERMISSIONS FULL
        THEN {
            RETURN {
                status: 200,
                body: "OK",
            }
        }
    
    COMMENT "test"
;

(INFO FOR DB).apis;
(INFO FOR DB).configs;